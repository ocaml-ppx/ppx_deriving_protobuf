open OUnit2

let suite = "Test ppx_protobuf" >::: [
    Test_protobuf.suite;
  ]

let _ =
  run_test_tt_main suite
