open OUnit2

type t = int * string [@@protobuf]
let test_tuple ctxt =
  let r = Protobuf.Decoder.of_string "\x12\x08spartans\x08\xac\x02" in
  assert_equal ~printer:(fun (x, y) -> Printf.sprintf "%d, %s" x y)
               (300, "spartans") (t_from_protobuf r)

let suite = "Test complex types" >::: [
    "test_tuple" >:: test_tuple;
  ]
