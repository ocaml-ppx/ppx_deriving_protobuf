open OUnit2
type uint32 = Uint32.t
type uint64 = Uint64.t

let assert_roundtrip printer encoder decoder str value =
  (* encode *)
  let e = Protobuf.Encoder.create () in
  encoder value e;
  assert_equal ~printer:(Printf.sprintf "%S") str (Protobuf.Encoder.to_string e);
  (* decode *)
  let d = Protobuf.Decoder.of_string str in
  assert_equal ~printer value (decoder d);

type b = bool [@@protobuf]
let test_bool ctxt =
  assert_roundtrip string_of_bool b_to_protobuf b_from_protobuf
                   "\x08\x01" true

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
  assert_roundtrip string_of_int i1_to_protobuf i1_from_protobuf
                   "\x08\xac\x02" 300;
  assert_roundtrip string_of_int i2_to_protobuf i2_from_protobuf
                   "\x08\xac\x02" 150;
  assert_roundtrip string_of_int i3_to_protobuf i3_from_protobuf
                   "\x0d\x2c\x01\x00\x00" 300;
  assert_roundtrip string_of_int i4_to_protobuf i4_from_protobuf
                   "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" 300;

  assert_roundtrip Int32.to_string il1_to_protobuf il1_from_protobuf
                   "\x08\xac\x02" 300l;
  assert_roundtrip Int32.to_string il2_to_protobuf il2_from_protobuf
                   "\x08\xac\x02" 150l;
  assert_roundtrip Int32.to_string il3_to_protobuf il3_from_protobuf
                   "\x0d\x2c\x01\x00\x00" 300l;
  assert_roundtrip Int32.to_string il4_to_protobuf il4_from_protobuf
                   "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" 300l;

  assert_roundtrip Int64.to_string iL1_to_protobuf iL1_from_protobuf
                   "\x08\xac\x02" 300L;
  assert_roundtrip Int64.to_string iL2_to_protobuf iL2_from_protobuf
                   "\x08\xac\x02" 150L;
  assert_roundtrip Int64.to_string iL3_to_protobuf iL3_from_protobuf
                   "\x0d\x2c\x01\x00\x00" 300L;
  assert_roundtrip Int64.to_string iL4_to_protobuf iL4_from_protobuf
                   "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" 300L

type ul1 = uint32 [@encoding varint] [@@protobuf]
type ul2 = uint32 [@encoding zigzag] [@@protobuf]
type ul3 = Uint32.t                  [@@protobuf]
type ul4 = uint32 [@encoding bits64] [@@protobuf]
type uL1 = uint64 [@encoding varint] [@@protobuf]
type uL2 = uint64 [@encoding zigzag] [@@protobuf]
type uL3 = uint64 [@encoding bits32] [@@protobuf]
type uL4 = Uint64.t                  [@@protobuf]
let test_uints ctxt =
  assert_roundtrip Uint32.to_string ul1_to_protobuf ul1_from_protobuf
                   "\x08\xac\x02" (Uint32.of_int 300);
  assert_roundtrip Uint32.to_string ul2_to_protobuf ul2_from_protobuf
                   "\x08\xac\x02" (Uint32.of_int 150);
  assert_roundtrip Uint32.to_string ul3_to_protobuf ul3_from_protobuf
                   "\x0d\x2c\x01\x00\x00" (Uint32.of_int 300);
  assert_roundtrip Uint32.to_string ul4_to_protobuf ul4_from_protobuf
                   "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" (Uint32.of_int 300);

  assert_roundtrip Uint64.to_string uL1_to_protobuf uL1_from_protobuf
                   "\x08\xac\x02" (Uint64.of_int 300);
  assert_roundtrip Uint64.to_string uL2_to_protobuf uL2_from_protobuf
                   "\x08\xac\x02" (Uint64.of_int 150);
  assert_roundtrip Uint64.to_string uL3_to_protobuf uL3_from_protobuf
                   "\x0d\x2c\x01\x00\x00" (Uint64.of_int 300);
  assert_roundtrip Uint64.to_string uL4_to_protobuf uL4_from_protobuf
                   "\x09\x2c\x01\x00\x00\x00\x00\x00\x00" (Uint64.of_int 300)

type f1 = float [@encoding bits32] [@@protobuf]
type f2 = float                    [@@protobuf]
let test_floats ctxt =
  assert_roundtrip string_of_float f1_to_protobuf f1_from_protobuf
                   "\x0d\x00\x00\xC0\x3f" 1.5;
  assert_roundtrip string_of_float f2_to_protobuf f2_from_protobuf
                   "\x09\x00\x00\x00\x00\x00\x00\xF8\x3f" 1.5

type s = string [@@protobuf]
let test_string ctxt =
  assert_roundtrip (fun x -> x) s_to_protobuf s_from_protobuf
                   "\x0a\x03abc" "abc"

type o = int option [@@protobuf]
let test_option ctxt =
  let printer x = match x with None -> "None" | Some v -> "Some " ^ (string_of_int v) in
  assert_roundtrip printer o_to_protobuf o_from_protobuf
                   "" None;
  assert_roundtrip printer o_to_protobuf o_from_protobuf
                   "\x08\xac\x02" (Some 300)

type l = int list [@@protobuf]
let test_list ctxt =
  let printer x = x |> List.map string_of_int |> String.concat ", " in
  assert_roundtrip printer l_to_protobuf l_from_protobuf
                   "" [] ;
  assert_roundtrip printer l_to_protobuf l_from_protobuf
                   "\x08\xac\x02\x08\x2a" [300; 42]

type a = int array [@@protobuf]
let test_array ctxt =
  let printer x = Array.to_list x |> List.map string_of_int |> String.concat ", " in
  assert_roundtrip printer a_to_protobuf a_from_protobuf
                   "" [||];
  assert_roundtrip printer a_to_protobuf a_from_protobuf
                   "\x08\xac\x02\x08\x2a" [|300; 42|]

type t = int * string [@@protobuf]
let test_tuple ctxt =
  let printer (x, y) = Printf.sprintf "%d, %s" x y in
  assert_roundtrip printer t_to_protobuf t_from_protobuf
                   "\x08\xac\x02\x12\x08spartans" (300, "spartans")

type r1 = {
  r1a : int    [@key 1];
  r1b : string [@key 2];
} [@@protobuf]
let test_record ctxt =
  let printer r = Printf.sprintf "{ r1a = %d, r1b = %s }" r.r1a r.r1b in
  assert_roundtrip printer r1_to_protobuf r1_from_protobuf
                   "\x08\xac\x02\x12\x08spartans"
                   { r1a = 300; r1b = "spartans" }

type r2 = {
  r2a : r1 [@key 1];
} [@@protobuf]
let test_nested ctxt =
  let printer r = Printf.sprintf "{ r2a = { r1a = %d, r1b = %s } }" r.r2a.r1a r.r2a.r1b in
  assert_roundtrip printer r2_to_protobuf r2_from_protobuf
                   "\x0a\x0d\x08\xac\x02\x12\x08spartans"
                   { r2a = { r1a = 300; r1b = "spartans" } }

type r3 = {
  r3a : (int [@encoding bits32] * string) [@key 1];
} [@@protobuf]
let test_imm_tuple ctxt =
  let printer { r3a = a, b } = Printf.sprintf "{ r3a = %d, %s } }" a b in
  assert_roundtrip printer r3_to_protobuf r3_from_protobuf
                   "\x0a\x0f\x0d\x2c\x01\x00\x00\x12\x08spartans"
                   { r3a = 300, "spartans" }

type v1 =
| V1A [@key 1]
| V1B [@key 2]
| V1C [@key 3] of int
| V1D [@key 4] of string * string
[@@protobuf]
let test_variant ctxt =
  let printer v =
    match v with
    | V1A -> "V1A"
    | V1B -> "V1B"
    | V1C i -> Printf.sprintf "V1C(%d)" i
    | V1D (s1,s2) -> Printf.sprintf "V1D(%S, %S)" s1 s2
  in
  assert_roundtrip printer v1_to_protobuf v1_from_protobuf
                   "\x08\x02" V1B;
  assert_roundtrip printer v1_to_protobuf v1_from_protobuf
                   "\x08\x03\x20\x2a" (V1C 42);
  assert_roundtrip printer v1_to_protobuf v1_from_protobuf
                   "\x08\x04\x2a\x0a\x0a\x03foo\x12\x03bar" (V1D ("foo", "bar"))

type v2 =
| V2A [@key 1]
| V2B [@key 2]
and r4 = {
  r4a : v2 [@key 1] [@bare]
} [@@protobuf]
let test_variant_bare ctxt =
  let printer { r4a } =
    match r4a with V2A -> "{ r4a = V2A }" | V2B -> "{ r4a = V2B }"
  in
  assert_roundtrip printer r4_to_protobuf r4_from_protobuf
                   "\x08\x02" { r4a = V2B }

type 'a r5 = {
  r5a: 'a [@key 1]
} [@@protobuf]
let test_tvar ctxt =
  let printer f { r5a } = Printf.sprintf "{ r5a = %s }" (f r5a) in
  assert_roundtrip (printer string_of_int)
                   (r5_to_protobuf i1_to_protobuf)
                   (r5_from_protobuf i1_from_protobuf)
                   "\x0a\x02\x08\x01" { r5a = 1 }

type 'a mylist =
| Nil  [@key 1]
| Cons [@key 2] of 'a * 'a mylist
[@@protobuf]
let test_mylist ctxt =
  let rec printer f v =
    match v with
    | Nil -> "Nil"
    | Cons (a, r) -> Printf.sprintf "Cons (%s, %s)" (f a) (printer f r)
  in
  assert_roundtrip (printer string_of_int)
                   (mylist_to_protobuf i1_to_protobuf)
                   (mylist_from_protobuf i1_from_protobuf)
                   ("\x08\x02\x1a\x1c\x0a\x02\x08\x01\x12\x16\x08\x02" ^
                    "\x1a\x12\x0a\x02\x08\x02\x12\x0c\x08\x02\x1a\x08" ^
                    "\x0a\x02\x08\x03\x12\x02\x08\x01")
                   (Cons (1, (Cons (2, (Cons (3, Nil))))))

let test_errors ctxt =
  (* scalars *)
  let d = Protobuf.Decoder.of_string "" in
  assert_raises Protobuf.Decoder.(Failure (Missing_field "Test_syntax.s"))
                (fun () -> s_from_protobuf d);
  let d = Protobuf.Decoder.of_string "\x0d\x00\x00\xC0\x3f" in
  assert_raises Protobuf.Decoder.(Failure (Unexpected_payload ("Test_syntax.s", Protobuf.Bits32)))
                (fun () -> s_from_protobuf d);
  (* records *)
  let d = Protobuf.Decoder.of_string "" in
  assert_raises Protobuf.Decoder.(Failure (Missing_field "Test_syntax.r1.r1b"))
                (fun () -> r1_from_protobuf d);
  (* tuples *)
  let d = Protobuf.Decoder.of_string "\x0a\x00" in
  assert_raises Protobuf.Decoder.(Failure (Missing_field "Test_syntax.r3.r3a/1"))
                (fun () -> r3_from_protobuf d);
  (* variants *)
  let d = Protobuf.Decoder.of_string "\x08\x03\x18\x1a" in
  assert_raises Protobuf.Decoder.(Failure (Malformed_variant "Test_syntax.v1"))
                (fun () -> v1_from_protobuf d)

let test_skip ctxt =
  let d = Protobuf.Decoder.of_string "\x15\x00\x00\xC0\x3f" in
  assert_raises Protobuf.Decoder.(Failure (Missing_field "Test_syntax.s"))
                (fun () -> s_from_protobuf d)

let suite = "Test syntax" >::: [
    "test_bool"         >:: test_bool;
    "test_ints"         >:: test_ints;
    "test_uints"        >:: test_uints;
    "test_floats"       >:: test_floats;
    "test_string"       >:: test_string;
    "test_option"       >:: test_option;
    "test_list"         >:: test_list;
    "test_array"        >:: test_array;
    "test_tuple"        >:: test_tuple;
    "test_record"       >:: test_record;
    "test_nested"       >:: test_nested;
    "test_imm_tuple"    >:: test_imm_tuple;
    "test_variant"      >:: test_variant;
    "test_variant_bare" >:: test_variant_bare;
    "test_tvar"         >:: test_tvar;
    "test_mylist"       >:: test_mylist;
    "test_errors"       >:: test_errors;
    "test_skip"         >:: test_skip;
  ]
