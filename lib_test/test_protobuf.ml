open OUnit2
open Protobuf

let test_reader ctxt =
  let r = Reader.of_string "\x01\x02" in
  assert_equal ~printer:string_of_bool true (Reader.bool r);
  assert_raises (Reader.Error Reader.Overflow) (fun () -> Reader.bool r);
  let r = Reader.of_string "\x01" in
  assert_equal ~printer:Int64.to_string 1L (Reader.varint r);
  let r = Reader.of_string "\xac\x02" in
  assert_equal ~printer:Int64.to_string 300L (Reader.varint r);
  let r = Reader.of_string "\x01\x02\x03\x04" in
  assert_equal ~printer:Int32.to_string 0x04030201l (Reader.bits32 r);
  let r = Reader.of_string "\x01\x02\x03\x04\x05\x06\x07\x08" in
  assert_equal ~printer:Int64.to_string 0x0807060504030201L (Reader.bits64 r);
  let r = Reader.of_string "\x03abc" in
  assert_equal ~printer:(fun x -> x) "abc" (Reader.bytes r);
  assert_raises (Reader.Error Reader.Incomplete) (fun () -> Reader.varint r);
  let r  = Reader.of_string "\x02\xac\x02" in
  let r' = Reader.nested r in
  assert_equal ~printer:Int64.to_string 300L (Reader.varint r');
  assert_raises (Reader.Error Reader.Incomplete) (fun () -> Reader.varint r');
  assert_raises (Reader.Error Reader.Incomplete) (fun () -> Reader.varint r);
  let r  = Reader.of_string "\x08\x11\x1a\x25" in
  assert_equal (Some (1, Varint)) (Reader.key r);
  assert_equal (Some (2, Bits64)) (Reader.key r);
  assert_equal (Some (3, Bytes))  (Reader.key r);
  assert_equal (Some (4, Bits32)) (Reader.key r);
  assert_equal None (Reader.key r);
  ()

let test_zigzag ctxt =
  let r = Reader.of_string "\x00" in
  assert_equal ~printer:Int64.to_string 0L (Reader.zigzag r);
  let r = Reader.of_string "\x01" in
  assert_equal ~printer:Int64.to_string (-1L) (Reader.zigzag r);
  let r = Reader.of_string "\x02" in
  assert_equal ~printer:Int64.to_string 1L (Reader.zigzag r);
  let r = Reader.of_string "\x03" in
  assert_equal ~printer:Int64.to_string (-2L) (Reader.zigzag r);
  ()

let test_overflow ctxt =
  if Sys.word_size = 32 then
    assert_raises (Reader.Error Reader.Overflow)
                  (fun () -> Reader.int_of_int32 0xffffffffl);
  assert_raises (Reader.Error Reader.Overflow)
                (fun () -> Reader.int_of_int64 0xffffffffffffffffL);
  ()

let suite = "Test Protobuf" >::: [
    "test_reader"   >:: test_reader;
    "test_zigzag"   >:: test_zigzag;
    "test_overflow" >:: test_overflow;
  ]
