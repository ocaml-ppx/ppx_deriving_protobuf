open OUnit2
open Protobuf

let test_reader ctxt =
  let d = Decoder.of_string "\x01\x02" in
  assert_equal ~printer:string_of_bool true (Decoder.bool d);
  assert_raises Decoder.(Failure Overflow) (fun () -> Decoder.bool d);
  let d = Decoder.of_string "\x01" in
  assert_equal ~printer:Int64.to_string 1L (Decoder.varint d);
  let d = Decoder.of_string "\xac\x02" in
  assert_equal ~printer:Int64.to_string 300L (Decoder.varint d);
  let d = Decoder.of_string "\x01\x02\x03\x04" in
  assert_equal ~printer:Int32.to_string 0x04030201l (Decoder.bits32 d);
  let d = Decoder.of_string "\x01\x02\x03\x04\x05\x06\x07\x08" in
  assert_equal ~printer:Int64.to_string 0x0807060504030201L (Decoder.bits64 d);
  let d = Decoder.of_string "\x03abc" in
  assert_equal ~printer:(fun x -> x) "abc" (Decoder.bytes d);
  assert_raises Decoder.(Failure Incomplete) (fun () -> Decoder.varint d);
  let d  = Decoder.of_string "\x02\xac\x02" in
  let d' = Decoder.nested d in
  assert_equal ~printer:Int64.to_string 300L (Decoder.varint d');
  assert_raises Decoder.(Failure Incomplete) (fun () -> Decoder.varint d');
  assert_raises Decoder.(Failure Incomplete) (fun () -> Decoder.varint d);
  let d  = Decoder.of_string "\x08\x11\x1a\x25" in
  assert_equal (Some (1, Varint)) (Decoder.key d);
  assert_equal (Some (2, Bits64)) (Decoder.key d);
  assert_equal (Some (3, Bytes))  (Decoder.key d);
  assert_equal (Some (4, Bits32)) (Decoder.key d);
  assert_equal None (Decoder.key d);
  let d  = Decoder.of_string "\x15\x00\x00\xC0\x3f\x01" in
  assert_equal (Some (2, Bits32)) (Decoder.key d);
  Decoder.skip d Bits32;
  assert_equal ~printer:Int64.to_string 1L (Decoder.varint d);
  ()

let test_zigzag ctxt =
  let d = Decoder.of_string "\x00" in
  assert_equal ~printer:Int64.to_string 0L (Decoder.zigzag d);
  let d = Decoder.of_string "\x01" in
  assert_equal ~printer:Int64.to_string (-1L) (Decoder.zigzag d);
  let d = Decoder.of_string "\x02" in
  assert_equal ~printer:Int64.to_string 1L (Decoder.zigzag d);
  let d = Decoder.of_string "\x03" in
  assert_equal ~printer:Int64.to_string (-2L) (Decoder.zigzag d);
  ()

let test_overflow ctxt =
  if Sys.word_size = 32 then
    assert_raises Decoder.(Failure Overflow)
                  (fun () -> Decoder.int_of_int32 0xffffffffl);
  assert_raises Decoder.(Failure Overflow)
                (fun () -> Decoder.int_of_int64 0xffffffffffffffffL);
  ()

let suite = "Test Protobuf" >::: [
    "test_reader"   >:: test_reader;
    "test_zigzag"   >:: test_zigzag;
    "test_overflow" >:: test_overflow;
  ]
