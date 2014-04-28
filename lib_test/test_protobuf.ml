open OUnit2
open Protobuf

let test_reader ctxt =
  let r = Decoder.of_string "\x01\x02" in
  assert_equal ~printer:string_of_bool true (Decoder.bool r);
  assert_raises Decoder.(Failure Overflow) (fun () -> Decoder.bool r);
  let r = Decoder.of_string "\x01" in
  assert_equal ~printer:Int64.to_string 1L (Decoder.varint r);
  let r = Decoder.of_string "\xac\x02" in
  assert_equal ~printer:Int64.to_string 300L (Decoder.varint r);
  let r = Decoder.of_string "\x01\x02\x03\x04" in
  assert_equal ~printer:Int32.to_string 0x04030201l (Decoder.bits32 r);
  let r = Decoder.of_string "\x01\x02\x03\x04\x05\x06\x07\x08" in
  assert_equal ~printer:Int64.to_string 0x0807060504030201L (Decoder.bits64 r);
  let r = Decoder.of_string "\x03abc" in
  assert_equal ~printer:(fun x -> x) "abc" (Decoder.bytes r);
  assert_raises Decoder.(Failure Incomplete) (fun () -> Decoder.varint r);
  let r  = Decoder.of_string "\x02\xac\x02" in
  let r' = Decoder.nested r in
  assert_equal ~printer:Int64.to_string 300L (Decoder.varint r');
  assert_raises Decoder.(Failure Incomplete) (fun () -> Decoder.varint r');
  assert_raises Decoder.(Failure Incomplete) (fun () -> Decoder.varint r);
  let r  = Decoder.of_string "\x08\x11\x1a\x25" in
  assert_equal (Some (1, Varint)) (Decoder.key r);
  assert_equal (Some (2, Bits64)) (Decoder.key r);
  assert_equal (Some (3, Bytes))  (Decoder.key r);
  assert_equal (Some (4, Bits32)) (Decoder.key r);
  assert_equal None (Decoder.key r);
  let r  = Decoder.of_string "\x15\x00\x00\xC0\x3f\x01" in
  assert_equal (Some (2, Bits32)) (Decoder.key r);
  Decoder.skip r Bits32;
  assert_equal ~printer:Int64.to_string 1L (Decoder.varint r);
  ()

let test_zigzag ctxt =
  let r = Decoder.of_string "\x00" in
  assert_equal ~printer:Int64.to_string 0L (Decoder.zigzag r);
  let r = Decoder.of_string "\x01" in
  assert_equal ~printer:Int64.to_string (-1L) (Decoder.zigzag r);
  let r = Decoder.of_string "\x02" in
  assert_equal ~printer:Int64.to_string 1L (Decoder.zigzag r);
  let r = Decoder.of_string "\x03" in
  assert_equal ~printer:Int64.to_string (-2L) (Decoder.zigzag r);
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
