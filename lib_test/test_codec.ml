open OUnit2
type uint32 = Uint32.t
type uint64 = Uint64.t

type b = bool [@@protobuf]
let test_bool ctxt =
  let d = Protobuf.Decoder.of_string "\x08\x01" in
  assert_equal ~printer:string_of_bool true (b_from_protobuf d)

type i1  = int                      [@@protobuf]
type i2  = int   [@encoding zigzag] [@@protobuf]
type i3  = int   [@encoding bits32] [@@protobuf]
type i4  = int   [@encoding bits64] [@@protobuf]
type il1 = int32 [@encoding varint] [@@protobuf]
type il2 = int32 [@encoding zigzag] [@@protobuf]
type il3 = Int32.t                  [@@protobuf]
type il4 = int32 [@encoding bits64] [@@protobuf]
type iL1 = int64 [@encoding varint] [@@protobuf]
type iL2 = int64 [@encoding zigzag] [@@protobuf]
type iL3 = int64 [@encoding bits32] [@@protobuf]
type iL4 = Int64.t                  [@@protobuf]
let test_ints ctxt =
  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:string_of_int 300 (i1_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:string_of_int 150 (i2_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x0d\x2c\x01\x00\x00" in
  assert_equal ~printer:string_of_int 300 (i3_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" in
  assert_equal ~printer:string_of_int 300 (i4_from_protobuf d);

  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:Int32.to_string 300l (il1_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:Int32.to_string 150l (il2_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x0d\x2c\x01\x00\x00" in
  assert_equal ~printer:Int32.to_string 300l (il3_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" in
  assert_equal ~printer:Int32.to_string 300l (il4_from_protobuf d);

  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:Int64.to_string 300L (iL1_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:Int64.to_string 150L (iL2_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x0d\x2c\x01\x00\x00" in
  assert_equal ~printer:Int64.to_string 300L (iL3_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" in
  assert_equal ~printer:Int64.to_string 300L (iL4_from_protobuf d)

type ul1 = uint32 [@encoding varint] [@@protobuf]
type ul2 = uint32 [@encoding zigzag] [@@protobuf]
type ul3 = Uint32.t                  [@@protobuf]
type ul4 = uint32 [@encoding bits64] [@@protobuf]
type uL1 = uint64 [@encoding varint] [@@protobuf]
type uL2 = uint64 [@encoding zigzag] [@@protobuf]
type uL3 = uint64 [@encoding bits32] [@@protobuf]
type uL4 = Uint64.t                  [@@protobuf]
let test_uints ctxt =
  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:Uint32.to_string (Uint32.of_int 300) (ul1_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:Uint32.to_string (Uint32.of_int 150) (ul2_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x0d\x2c\x01\x00\x00" in
  assert_equal ~printer:Uint32.to_string (Uint32.of_int 300) (ul3_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" in
  assert_equal ~printer:Uint32.to_string (Uint32.of_int 300) (ul4_from_protobuf d);

  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:Uint64.to_string (Uint64.of_int 300) (uL1_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal ~printer:Uint64.to_string (Uint64.of_int 150) (uL2_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x0d\x2c\x01\x00\x00" in
  assert_equal ~printer:Uint64.to_string (Uint64.of_int 300) (uL3_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" in
  assert_equal ~printer:Uint64.to_string (Uint64.of_int 300) (uL4_from_protobuf d)

type f1 = float [@encoding bits32] [@@protobuf]
type f2 = float                    [@@protobuf]
let test_floats ctxt =
  let d = Protobuf.Decoder.of_string "\x0d\x00\x00\xC0\x3f" in
  assert_equal ~printer:string_of_float 1.5 (f1_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x09\x00\x00\x00\x00\x00\x00\xF8\x3f" in
  assert_equal ~printer:string_of_float 1.5 (f2_from_protobuf d)

type s = string [@@protobuf]
let test_string ctxt =
  let d = Protobuf.Decoder.of_string "\x0a\x03abc" in
  assert_equal ~printer:(fun x -> x) "abc" (s_from_protobuf d)

type o = int option [@@protobuf]
let test_option ctxt =
  let d = Protobuf.Decoder.of_string "" in
  assert_equal None (o_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x08\xac\x02" in
  assert_equal (Some 300) (o_from_protobuf d)

type l = int list [@@protobuf]
let test_list ctxt =
  let printer x = x |> List.map string_of_int |> String.concat ", " in
  let d = Protobuf.Decoder.of_string "" in
  assert_equal ~printer [] (l_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x08\xac\x02\x08\x2a" in
  assert_equal ~printer [300; 42] (l_from_protobuf d)

type a = int array [@@protobuf]
let test_array ctxt =
  let printer x = Array.to_list x |> List.map string_of_int |> String.concat ", " in
  let d = Protobuf.Decoder.of_string "" in
  assert_equal ~printer [||] (a_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x08\xac\x02\x08\x2a" in
  assert_equal ~printer [|300; 42|] (a_from_protobuf d)

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

type r2 = {
  r2a : r1 [@key 1];
} [@@protobuf]
let test_nested ctxt =
  let d = Protobuf.Decoder.of_string "\x0a\x0d\x12\x08spartans\x08\xac\x02" in
  assert_equal ~printer:(fun r -> Printf.sprintf "{ a = { a = %d, b = %s } }" r.r2a.r1a r.r2a.r1b)
               { r2a = { r1a = 300; r1b = "spartans" } } (r2_from_protobuf d)

type r3 = {
  r3a : (int [@encoding bits32] * string) [@key 1];
} [@@protobuf]
let test_imm_tuple ctxt =
  let d = Protobuf.Decoder.of_string "\x0a\x0f\x12\x08spartans\x0d\x2c\x01\x00\x00" in
  assert_equal ~printer:(fun { r3a = a, b } -> Printf.sprintf "{ a = %d, %s } }" a b)
               { r3a = 300, "spartans" } (r3_from_protobuf d)

let test_errors ctxt =
  let d = Protobuf.Decoder.of_string "" in
  assert_raises Protobuf.Decoder.(Failure (Missing_field "s"))
                (fun () -> s_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x0d\x00\x00\xC0\x3f" in
  assert_raises Protobuf.Decoder.(Failure (Unexpected_payload ("s", Bits32)))
                (fun () -> s_from_protobuf d)

let test_skip ctxt =
  let d = Protobuf.Decoder.of_string "\x15\x00\x00\xC0\x3f" in
  assert_raises Protobuf.Decoder.(Failure (Missing_field "s"))
                (fun () -> s_from_protobuf d)

let suite = "Test primitive types" >::: [
    "test_bool"       >:: test_bool;
    "test_ints"       >:: test_ints;
    "test_uints"      >:: test_uints;
    "test_floats"     >:: test_floats;
    "test_string"     >:: test_string;
    "test_option"     >:: test_option;
    "test_list"       >:: test_list;
    "test_array"      >:: test_array;
    "test_tuple"      >:: test_tuple;
    "test_record"     >:: test_record;
    "test_nested"     >:: test_nested;
    "test_imm_tuple"  >:: test_imm_tuple;
    "test_errors"     >:: test_errors;
    "test_skip"       >:: test_skip;
  ]

