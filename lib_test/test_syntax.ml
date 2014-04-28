open OUnit2

type t1 = int [@key 1] [@@protobuf]

let test_t1 ctxt =
  let r = Protobuf.Reader.of_string "\x08\xac\x02" in
  assert_equal 300 (t1_from_protobuf r)

let suite = "Test syntax" >::: [
    "test_t1" >:: test_t1
  ]
