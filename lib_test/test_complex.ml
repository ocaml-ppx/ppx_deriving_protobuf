open OUnit2

type t = int * string [@@protobuf]
let test_tuple ctxt =
  let d = Protobuf.Decoder.of_string "\x12\x08spartans\x08\xac\x02" in
  assert_equal ~printer:(fun (x, y) -> Printf.sprintf "%d, %s" x y)
               (300, "spartans") (t_from_protobuf d)

type r1 = {
  r1a : int    [@key 1];
  r1b : string [@key 2];
} [@@protobuf]
let test_record ctxt =
  let d = Protobuf.Decoder.of_string "\x12\x08spartans\x08\xac\x02" in
  assert_equal ~printer:(fun r -> Printf.sprintf "{ a = %d, b = %s }" r.r1a r.r1b)
               { r1a = 300; r1b = "spartans" } (r1_from_protobuf d)

let suite = "Test complex types" >::: [
    "test_tuple"  >:: test_tuple;
    "test_record" >:: test_record;
  ]
