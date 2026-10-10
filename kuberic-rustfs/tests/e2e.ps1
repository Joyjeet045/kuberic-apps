param(
    [string]$Kind = "kind",
    [string]$Image = "kuberic-rustfs:local",
    [int]$ObjectCount = 128
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
if ($ObjectCount -lt 32) { throw "At least 32 objects are required for interruption coverage" }
$kubectl = (Get-Command kubectl -ErrorAction Stop).Source
$curl = (Get-Command $(if ($IsWindows) { "curl.exe" } else { "curl" }) -ErrorAction Stop).Source
$run = [Guid]::NewGuid().ToString("N").Substring(0, 10)
$cluster = "rustfs-$run"
$root = Join-Path ([IO.Path]::GetTempPath()) "kuberic-rustfs-$run"
$null = New-Item -ItemType Directory $root
$kubeconfig = Join-Path $root "kubeconfig"
$forwards = @{}
$created = $false
$poolA = "http://rfs-a-{0...3}.rustfs-internal:9000/storage/data"
$poolB = "http://rfs-b-{0...3}.rustfs-internal:9000/storage/data"
$access = "e2e-$run"
$secret = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$token = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$responseFile = Join-Path $root "response.bin"
$payloadFile = Join-Path $root "payload.bin"
$payload = [Security.Cryptography.RandomNumberGenerator]::GetBytes(1024 * 1024)
[IO.File]::WriteAllBytes($payloadFile, $payload)
$payloadHashes = @{}

function Native([string]$Executable, [string[]]$Arguments) {
    $output = & $Executable @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) { throw "$Executable failed ($LASTEXITCODE): $($output -join "`n")" }
    return $output -join "`n"
}

function K([string[]]$Arguments) {
    Native $kubectl (@("--kubeconfig", $kubeconfig, "--request-timeout=30s", "-n", "rustfs-example") + $Arguments)
}

function Wait-For([string]$Label, [scriptblock]$Condition, [int]$Seconds = 180) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $last = "condition not yet satisfied"
    while ($watch.Elapsed.TotalSeconds -lt $Seconds) {
        try {
            if (& $Condition) { Write-Host "PASS $Label"; return }
        } catch {
            $last = $_.Exception.Message
            Write-Host "WAIT $Label - $last"
        }
        Start-Sleep -Milliseconds 500
    }
    throw "$Label exceeded $Seconds seconds: $last"
}

function Forward([string]$Pod) {
    if ($forwards.ContainsKey($Pod)) {
        $old = $forwards[$Pod].Process
        if (-not $old.HasExited) { Stop-Process -Id $old.Id -Force; $old.WaitForExit() }
    }
    $entry = @{}
    $log = Join-Path $root "forward-$Pod-$([Guid]::NewGuid().ToString('N')).log"
    $arguments = @(
        "--kubeconfig", "`"$kubeconfig`"", "-n", "rustfs-example",
        "port-forward", "--address=127.0.0.1", "pod/$Pod",
        ":9000", ":9002", ":9003"
    )
    $entry.Process = Start-Process -FilePath $kubectl -ArgumentList $arguments -PassThru `
        -RedirectStandardOutput $log -RedirectStandardError "$log.err"
    $forwards[$Pod] = $entry
    Wait-For "port-forward $Pod" {
        if ($entry.Process.HasExited) { throw (Get-Content "$log.err" -Raw) }
        if (-not (Test-Path $log)) { return $false }
        $text = Get-Content $log -Raw
        if (-not $text) { return $false }
        foreach ($port in @{ Native = 9000; Client = 9002; Control = 9003 }.GetEnumerator()) {
            if ($text -notmatch "Forwarding from 127\.0\.0\.1:(\d+) -> $($port.Value)") { return $false }
            $entry[$port.Key] = [int]$Matches[1]
        }
        $true
    } 30
    return $entry
}

function Http([int]$Port, [string]$Path, [string]$Method = "GET", [string]$Json = "", [string]$File = "", [switch]$Signed, [switch]$Control) {
    $arguments = @("--silent", "--show-error", "--max-time", "15", "--output", $responseFile,
        "--write-out", "%{http_code}", "--request", $Method, "http://127.0.0.1:$Port$Path")
    if ($Signed) { $arguments += @("--aws-sigv4", "aws:amz:us-east-1:s3", "--user", "${access}:$secret") }
    if ($Control) { $arguments += @("--header", "Authorization: Bearer $token") }
    if ($Json) {
        $requestFile = Join-Path $root "request.json"
        [IO.File]::WriteAllText($requestFile, $Json)
        $arguments += @("--header", "Content-Type: application/json", "--data-binary", "@$requestFile")
    }
    if ($File) { $arguments += @("--data-binary", "@$File") }
    $code = & $curl @arguments 2> (Join-Path $root "curl.err")
    if ($LASTEXITCODE -ne 0) {
        return @{ Status = 0; Body = [byte[]]@(); Text = (Get-Content (Join-Path $root "curl.err") -Raw) }
    }
    $bytes = [IO.File]::ReadAllBytes($responseFile)
    return @{ Status = [int]$code; Body = $bytes; Text = [Text.Encoding]::UTF8.GetString($bytes) }
}

function Expect-Status($Response, [int]$Status) {
    if ($Response.Status -ne $Status) { throw "Expected HTTP $Status, got $($Response.Status): $($Response.Text)" }
}

function Observe([string]$Pod) {
    $result = Http $forwards[$Pod].Control "/v1/native/observation" -Control
    Expect-Status $result 200
    return $result.Text | ConvertFrom-Json -AsHashtable
}

function Topology([string]$Pod, [string[]]$Pools) {
    return @{ pools = $Pools; local_node = "http://$Pod.rustfs-internal:9000"; erasure_set_drive_count = 4 }
}

function Set-Plan([long]$Revision, [string[]]$Pods, [hashtable]$Operations = @{}) {
    $nodes = @($Pods | ForEach-Object {
        @{ node = "http://$_.rustfs-internal:9003"; operation = $Operations[$_] }
    })
    $config = @{ token_file = "/credentials/control-token"; plan = @{
        revision = $Revision; enabled = $true; lease_millis = 10000; nodes = $nodes
    }}
    $path = Join-Path $root "controller.json"
    $config | ConvertTo-Json -Depth 30 | Set-Content -Encoding utf8NoBOM $path
    $yaml = K @("create", "configmap", "rustfs-controller", "--from-file=controller.json=$path", "--dry-run=client", "-o", "yaml")
    $yaml | & $kubectl --kubeconfig $kubeconfig -n rustfs-example apply -f -
    if ($LASTEXITCODE -ne 0) { throw "Controller plan update failed" }
}

function Await-Plan([long]$Revision, [string[]]$Pods, [int]$Seconds = 240) {
    Wait-For "native plan $Revision" {
        foreach ($pod in $Pods) {
            $observed = Observe $pod
            if ($observed.revision -ne (2 * $Revision + 1) -or -not $observed.accepting_clients) { return $false }
        }
        return $true
    } $Seconds
}

function Inventory([string]$Pod) {
    for ($i = 0; $i -lt $ObjectCount; $i++) {
        $result = Http $forwards[$Pod].Client "/inventory/object-$i" -Signed
        Expect-Status $result 200
        if ([Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([byte[]]$result.Body)) -ne $payloadHashes[$i]) {
            throw "Acknowledged object-$i changed through $Pod"
        }
    }
    Write-Host "PASS all $ObjectCount acknowledged objects through $Pod"
}

function Native-Pid([string]$Pod) {
    $script = 'for f in /proc/1/task/*/children; do for p in $(cat "$f"); do if [ "$(cat /proc/$p/comm)" = rustfs ]; then echo "$p"; fi; done; done'
    $value = (K @("exec", $Pod, "-c", "adapter", "--", "/bin/sh", "-c", $script)).Trim()
    if ($value -notmatch "^\d+$" -or [int]$value -le 1) { throw "Could not resolve the owned foreground RustFS PID: $value" }
    return [int]$value
}

function Signal-Native([string]$Pod, [int]$ProcessId, [string]$Signal) {
    K @("exec", $Pod, "-c", "adapter", "--", "/bin/sh", "-c", "kill -$Signal $ProcessId") | Write-Host
}

function Operation-Status([string]$Pod, [hashtable]$Operation) {
    $result = Http $forwards[$Pod].Control "/v1/native/operation/status" "POST" ($Operation | ConvertTo-Json -Depth 30 -Compress) -Control
    Expect-Status $result 200
    return $result.Text | ConvertFrom-Json -AsHashtable
}

try {
    Native $Kind @("version") | Write-Host
    Native "docker" @("image", "inspect", $Image, "--format", "{{.Id}}") | Write-Host
    $kindConfig = Join-Path $root "kind.yaml"
    @"
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  disableDefaultCNI: true
  podSubnet: 192.168.0.0/16
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
  - role: worker
"@ | Set-Content -Encoding utf8NoBOM $kindConfig
    $created = $true
    Native $Kind @("create", "cluster", "--name", $cluster, "--kubeconfig", $kubeconfig,
        "--config", $kindConfig, "--image", "kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed") | Write-Host
    Native $Kind @("load", "docker-image", $Image, "--name", $cluster) | Write-Host
    $cniManifest = Join-Path $root "calico.yaml"
    Native $curl @("--fail", "--location", "--silent", "--show-error", "--output", $cniManifest,
        "https://raw.githubusercontent.com/projectcalico/calico/v3.33.0/manifests/calico.yaml") | Write-Host
    $cniImages = @([regex]::Matches((Get-Content $cniManifest -Raw), '(?m)^\s+image:\s*(\S+)') |
        ForEach-Object { $_.Groups[1].Value.Trim('"').Trim("'") } | Sort-Object -Unique)
    if ($cniImages.Count -eq 0) { throw "The pinned CNI manifest contains no images" }
    foreach ($cniImage in $cniImages) { Native "docker" @("pull", "--platform=linux/amd64", $cniImage) | Write-Host }
    $cniArchive = Join-Path $root "cni.tar"
    Native "docker" (@("image", "save", "--platform=linux/amd64", "--output", $cniArchive) + $cniImages) | Write-Host
    Native $Kind @("load", "image-archive", $cniArchive, "--name", $cluster) | Write-Host
    Native $kubectl @("--kubeconfig", $kubeconfig, "apply", "-f", $cniManifest) | Write-Host
    Native $kubectl @("--kubeconfig", $kubeconfig, "-n", "kube-system", "rollout",
        "status", "daemonset/calico-node", "--timeout=600s") | Write-Host
    K @("wait", "--for=condition=Ready", "nodes", "--all", "--timeout=300s") | Write-Host
    K @("create", "namespace", "rustfs-example") | Write-Host
    foreach ($item in @(@("access-key", $access), @("secret-key", $secret), @("control-token", $token))) {
        [IO.File]::WriteAllText((Join-Path $root $item[0]), $item[1])
    }
    K @("create", "secret", "generic", "rustfs-credentials",
        "--from-file=access-key=$(Join-Path $root 'access-key')",
        "--from-file=secret-key=$(Join-Path $root 'secret-key')",
        "--from-file=control-token=$(Join-Path $root 'control-token')") | Write-Host
    $base = Join-Path $root "base.yaml"
    (Get-Content (Join-Path $PSScriptRoot "..\deploy\base.yaml") -Raw).Replace("kuberic-rustfs:local", $Image) |
        Set-Content -Encoding utf8NoBOM $base
    K @("apply", "-f", $base) | Write-Host
    $podsA = @("rfs-a-0", "rfs-a-1", "rfs-a-2", "rfs-a-3")
    Wait-For "all initial Pods exist" {
        @((K @("get", "pods", "-l", "rustfs-pool=a", "-o", "json") | ConvertFrom-Json).items).Count -eq 4
    }
    K @("wait", "--for=condition=Ready", "pod", "-l", "rustfs-pool=a", "--timeout=300s") | Write-Host
    foreach ($pod in $podsA) { $null = Forward $pod }
    Await-Plan 1 $podsA
    Expect-Status (Http $forwards["rfs-a-0"].Control "/v1/native/observation") 401
    Expect-Status (Http $forwards["rfs-a-0"].Client "/inventory" "PUT" -Signed) 200
    for ($i = 0; $i -lt $ObjectCount; $i++) {
        [BitConverter]::GetBytes($i).CopyTo($payload, 0)
        [IO.File]::WriteAllBytes($payloadFile, $payload)
        $payloadHashes[$i] = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($payload))
        Expect-Status (Http $forwards["rfs-a-0"].Client "/inventory/object-$i" "PUT" -File $payloadFile -Signed) 200
    }
    Inventory "rfs-a-1"
    K @("run", "network-probe", "--image=$Image", "--restart=Never", "--command", "--", "/bin/sleep", "infinity") | Write-Host
    K @("wait", "--for=condition=Ready", "pod/network-probe", "--timeout=120s") | Write-Host
    K @("exec", "network-probe", "--", "/usr/bin/timeout", "5", "/bin/bash", "-c", "</dev/tcp/rfs-a-0.rustfs-internal/9002") | Write-Host
    foreach ($port in @(9000, 9003)) {
        & $kubectl --kubeconfig $kubeconfig -n rustfs-example exec network-probe -- /usr/bin/timeout 5 /bin/bash -c "</dev/tcp/rfs-a-0.rustfs-internal/$port"
        if ($LASTEXITCODE -ne 124) { throw "NetworkPolicy did not block direct port $port access" }
    }
    Write-Host "PASS enforced client/native network separation"
    K @("scale", "deployment/rustfs-controller", "--replicas=0") | Write-Host
    Wait-For "controller loss expires client leases" {
        -not (Observe "rfs-a-0").accepting_clients
    } 45
    if ((Http $forwards["rfs-a-0"].Client "/inventory/object-0" -Signed).Status -ne 0) { throw "Fenced gateway still forwarded client traffic" }
    Expect-Status (Http $forwards["rfs-a-0"].Native "/minio/health/cluster") 200
    K @("scale", "deployment/rustfs-controller", "--replicas=1") | Write-Host
    Await-Plan 1 $podsA
    $invalid = @{
        authority = @{ incarnation = (Observe "rfs-a-0").incarnation; revision = 4; enabled = $false; lease_millis = 10000 }
        operation = @{ id = "invalid-shrink"; request = @{
            kind = "expand"; previous = Topology "rfs-a-0" @($poolA)
            target = Topology "rfs-a-0" @("http://rfs-a-{0...1}.rustfs-internal:9000/storage/data")
        }}
    }
    Expect-Status (Http $forwards["rfs-a-0"].Control "/v1/native/operation" "POST" ($invalid | ConvertTo-Json -Depth 30 -Compress) -Control) 409
    $restart = @{ id = "restart-2"; request = @{ kind = "restart"; topology = Topology "rfs-a-0" @($poolA) } }
    Set-Plan 2 $podsA @{"rfs-a-0" = $restart}
    Await-Plan 2 $podsA
    $receipt = Operation-Status "rfs-a-0" $restart
    if ($receipt.state -ne "complete") { throw "Restart did not persist native evidence" }
    $conflict = @{ id = $restart.id; request = @{ kind = "unsupported" } }
    Expect-Status (Http $forwards["rfs-a-0"].Control "/v1/native/operation/status" "POST" ($conflict | ConvertTo-Json -Depth 30 -Compress) -Control) 409
    Inventory "rfs-a-0"
    $stale = @{ incarnation = (Observe "rfs-a-0").incarnation; revision = 1; enabled = $true; lease_millis = 10000 }
    Expect-Status (Http $forwards["rfs-a-0"].Control "/v1/native/authority" "POST" ($stale | ConvertTo-Json -Compress) -Control) 409
    K @("scale", "statefulset/rfs-a", "--replicas=2") | Write-Host
    K @("wait", "--for=delete", "pod/rfs-a-2", "pod/rfs-a-3", "--timeout=90s") | Write-Host
    Wait-For "native write quorum loss is observed" {
        $o = Observe "rfs-a-0"
        $o.health.state -eq "observed" -and -not $o.health.health.writable
    } 90
    $nativeRead = Http $forwards["rfs-a-0"].Native "/minio/health/cluster/read"
    if ($nativeRead.Status -notin @(200, 503)) { throw "Native read health returned $($nativeRead.Status): $($nativeRead.Text)" }
    $observed = Observe "rfs-a-0"
    if ($observed.health.state -ne "observed" -or $observed.health.health.readable -ne ($nativeRead.Status -eq 200)) {
        throw "Adapter read health differs from the native verdict: $($observed | ConvertTo-Json -Depth 10 -Compress)"
    }
    Expect-Status (Http $forwards["rfs-a-0"].Client "/inventory/object-0" -Signed) 200
    Expect-Status (Http $forwards["rfs-a-0"].Client "/inventory/uncommitted" "PUT" -File $payloadFile -Signed) 503
    K @("scale", "statefulset/rfs-a", "--replicas=1") | Write-Host
    K @("wait", "--for=delete", "pod/rfs-a-1", "--timeout=90s") | Write-Host
    Wait-For "native read quorum loss is not reported healthy" {
        $o = Observe "rfs-a-0"
        $o.health.state -eq "observed" -and -not $o.health.health.readable -and -not $o.health.health.writable
    } 90
    Expect-Status (Http $forwards["rfs-a-0"].Client "/inventory/object-0" -Signed) 503
    K @("scale", "statefulset/rfs-a", "--replicas=4") | Write-Host
    Wait-For "all quorum participants return" {
        @((K @("get", "pods", "-l", "rustfs-pool=a", "-o", "json") | ConvertFrom-Json).items).Count -eq 4
    }
    K @("wait", "--for=condition=Ready", "pod", "-l", "rustfs-pool=a", "--timeout=240s") | Write-Host
    foreach ($pod in @("rfs-a-1", "rfs-a-2", "rfs-a-3")) { $null = Forward $pod }
    Wait-For "native quorum restored" {
        $o = Observe "rfs-a-0"
        $o.health.state -eq "observed" -and $o.health.health.writable
    } 120
    $old = Observe "rfs-a-0"
    $oldUid = (K @("get", "pod/rfs-a-0", "-o", "json") | ConvertFrom-Json).metadata.uid
    K @("delete", "pod/rfs-a-0", "--grace-period=0", "--force", "--wait=true") | Write-Host
    Wait-For "replacement Pod is ready on the same PVC" {
        $pod = K @("get", "pod/rfs-a-0", "-o", "json") | ConvertFrom-Json
        $pod.metadata.uid -ne $oldUid -and ($pod.status.conditions | Where-Object type -eq "Ready").status -eq "True"
    } 240
    $null = Forward "rfs-a-0"
    Await-Plan 2 $podsA
    $oldAuthority = @{ incarnation = $old.incarnation; revision = 999; enabled = $true; lease_millis = 10000 }
    Expect-Status (Http $forwards["rfs-a-0"].Control "/v1/native/authority" "POST" ($oldAuthority | ConvertTo-Json -Compress) -Control) 409
    if ((Operation-Status "rfs-a-0" $restart | ConvertTo-Json -Depth 30 -Compress) -ne ($receipt | ConvertTo-Json -Depth 30 -Compress)) {
        throw "Restart receipt changed after same-PVC reopening"
    }
    Inventory "rfs-a-0"
    $expand = Join-Path $root "expand.yaml"
    (Get-Content (Join-Path $PSScriptRoot "..\deploy\expand.yaml") -Raw).Replace("kuberic-rustfs:local", $Image) |
        Set-Content -Encoding utf8NoBOM $expand
    K @("apply", "-f", $expand) | Write-Host
    $podsB = @("rfs-b-0", "rfs-b-1", "rfs-b-2", "rfs-b-3")
    foreach ($pod in $podsB) {
        Wait-For "new pool process $pod" {
            (K @("get", "pod/$pod", "-o", "json") | ConvertFrom-Json).status.phase -eq "Running"
        } 180
        $null = Forward $pod
    }
    $all = $podsA + $podsB
    $operations = @{}
    foreach ($pod in $podsA) {
        $operations[$pod] = @{ id = "expand-3"; request = @{
            kind = "expand"; previous = Topology $pod @($poolA); target = Topology $pod @($poolA, $poolB)
        }}
    }
    Set-Plan 3 $all $operations
    Await-Plan 3 $all 300
    Inventory "rfs-b-0"
    $operations = @{}
    foreach ($pod in $all) {
        $operations[$pod] = @{ id = "decommission-4"; request = @{
            kind = "decommission"; topology = Topology $pod @($poolA, $poolB); pool = 0
        }}
    }
    $decommissionPid = Native-Pid "rfs-a-0"
    Set-Plan 4 $all $operations
    Wait-For "decommission is running before interruption" {
        $r = Http $forwards["rfs-a-0"].Native "/rustfs/admin/v3/decommission/status" -Signed
        Expect-Status $r 200
        $pool = ($r.Text | ConvertFrom-Json -AsHashtable).pools | Where-Object id -eq 0
        $pool.status -eq "running" -and $pool.decommissionInfo.objectsDecommissioned -gt 0 -and
            $pool.decommissionInfo.objectsDecommissioned -lt $ObjectCount
    } 120
    Signal-Native "rfs-a-0" $decommissionPid "STOP"
    if ((Operation-Status "rfs-a-0" $operations["rfs-a-0"]).state -ne "pending") {
        throw "Decommission completed before its participant could be interrupted"
    }
    $oldUid = (K @("get", "pod/rfs-a-0", "-o", "json") | ConvertFrom-Json).metadata.uid
    K @("delete", "pod/rfs-a-0", "--grace-period=0", "--force", "--wait=true") | Write-Host
    Wait-For "interrupted operation participant returns" {
        $pod = K @("get", "pod/rfs-a-0", "-o", "json") | ConvertFrom-Json
        $pod.metadata.uid -ne $oldUid -and $pod.status.phase -eq "Running"
    } 180
    $null = Forward "rfs-a-0"
    Await-Plan 4 $all 360
    foreach ($pod in $all) {
        $status = Operation-Status $pod $operations[$pod]
        if ($status.state -ne "complete" -or $status.evidence.status -ne "complete" -or
            $status.evidence.poolStatus -ne "decommissioned" -or
            $status.evidence.decommissionInfo.objectsDecommissioned -lt $ObjectCount -or
            $status.evidence.decommissionInfo.bytesDecommissioned -lt ($ObjectCount * $payload.Length)) {
            throw "Decommission lacks complete native movement evidence on $pod"
        }
    }
    Inventory "rfs-b-1"
    $node = (K @("get", "pod/rfs-b-0", "-o", "json") | ConvertFrom-Json).spec.nodeName
    $ownedNodes = (Native $Kind @("get", "nodes", "--name", $cluster)) -split "`n"
    if ($node -notin $ownedNodes) { throw "Refusing to stop a node outside the isolated test cluster" }
    Native "docker" @("stop", "--timeout", "0", $node) | Write-Host
    $survivor = $podsB | Where-Object { (K @("get", "pod/$_", "-o", "json") | ConvertFrom-Json).spec.nodeName -ne $node } | Select-Object -First 1
    Wait-For "native writes survive one worker outage" {
        Expect-Status (Http $forwards[$survivor].Client "/inventory/worker-outage" "PUT" -File $payloadFile -Signed) 200
        $true
    } 90
    Native "docker" @("start", $node) | Write-Host
    K @("wait", "--for=condition=Ready", "node/$node", "--timeout=180s") | Write-Host
    K @("wait", "--for=condition=Ready", "pod", "-l", "app.kubernetes.io/name=rustfs", "--timeout=240s") | Write-Host
    foreach ($pod in $all) {
        if ((K @("get", "pod/$pod", "-o", "json") | ConvertFrom-Json).spec.nodeName -eq $node) { $null = Forward $pod }
    }
    Await-Plan 4 $all
    Inventory "rfs-b-0"
    $outageObject = Http $forwards["rfs-b-0"].Client "/inventory/worker-outage" -Signed
    Expect-Status $outageObject 200
    if ([Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([byte[]]$outageObject.Body)) -ne $payloadHashes[$ObjectCount - 1]) {
        throw "Object acknowledged during worker outage changed after rejoining"
    }
    Write-Host "PASS complete RustFS native/Kuberic Kubernetes end-to-end suite"
} finally {
    if ($created) {
        try {
            K @("get", "pods", "-o", "wide") | Write-Host
            $pods = (K @("get", "pods", "-l", "app.kubernetes.io/name=rustfs", "-o", "json") | ConvertFrom-Json).items
            foreach ($pod in $pods) {
                Write-Host "Diagnostics: $($pod.metadata.name)"
                $logs = (K @("logs", $pod.metadata.name, "-c", "adapter", "--tail=2000")) -split "`n" |
                    Where-Object { $_ -notmatch '"event":"http_request_completed"' }
                $logs | Select-Object -First 30 | Write-Host
                $logs | Select-Object -Last 30 | Write-Host
            }
            K @("logs", "deployment/rustfs-controller", "--tail=20") | Write-Host
        } catch { Write-Warning "Could not collect all test diagnostics: $_" }
    }
    foreach ($entry in $forwards.Values) {
        if (-not $entry.Process.HasExited) { Stop-Process -Id $entry.Process.Id -Force; $entry.Process.WaitForExit() }
    }
    if ($created) { Native $Kind @("delete", "cluster", "--name", $cluster) | Write-Host }
    Get-ChildItem -LiteralPath $root -File | ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force }
    Remove-Item -LiteralPath $root
}
