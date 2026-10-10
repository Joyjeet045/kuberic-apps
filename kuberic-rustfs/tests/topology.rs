use kuberic_rustfs::Topology;

#[test]
fn derives_local_volumes_without_reordering_pools_or_ranges() {
    let topology = Topology {
        pools: vec![
            "http://node{1...4}:9000/data{1...2}".into(),
            "http://node{1...4}:9000/data{3...4}".into(),
        ],
        local_node: Some("http://node2:9000".into()),
        erasure_set_drive_count: Some(4),
    };
    assert_eq!(
        topology.local_volumes().unwrap(),
        ["/data1", "/data2", "/data3", "/data4"].map(std::path::PathBuf::from)
    );
}

#[test]
fn rejects_unsafe_or_unsupported_topologies() {
    for (pools, node, width) in [
        (vec![], None, None),
        (vec!["relative"], None, None),
        (
            vec!["http://node{1...4}:9000/data"],
            Some("http://missing:9000"),
            None,
        ),
        (
            vec!["http://node{1...4}:9000/data"],
            Some("http://node1:9000/path"),
            None,
        ),
        (
            vec!["http://node{1...4}:9000/data"],
            Some("https://node1:9000"),
            None,
        ),
        (
            vec!["http://user:secret@node{1...4}:9000/data"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{1...4}:9000/data?x=1"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{1...4}:9000/a/../data"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{1...4}:9000/%64ata"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{1...4}:9000/"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node1:9000/data"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{1...4}:9000/data"],
            Some("http://node1:9000"),
            Some(3),
        ),
        (
            vec!["http://node{1...4}:9000/data"],
            Some("http://node1:9000"),
            Some(1),
        ),
        (
            vec![
                "http://node{1...4}:9000/data",
                "http://node{1...4}:9000/data",
            ],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{4...1}:9000/data"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{1...1}:9000/data"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{01...04}:9000/data"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{1...1000000}:9000/data"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{1..4}:9000/data"],
            Some("http://node1:9000"),
            None,
        ),
        (
            vec!["http://node{1...4:9000/data"],
            Some("http://node1:9000"),
            None,
        ),
    ] {
        let topology = Topology {
            pools: pools.into_iter().map(String::from).collect(),
            local_node: node.map(String::from),
            erasure_set_drive_count: width,
        };
        assert!(topology.local_volumes().is_err(), "accepted {topology:?}");
    }
}
