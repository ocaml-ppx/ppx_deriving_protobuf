open OUnit2
open Protobuf

let test_xforms ctxt =
  assert_equal  0l (zigzag32 0l);
  assert_equal  1l (zigzag32 (-1l));
  assert_equal  2l (zigzag32 1l);
  assert_equal  3l (zigzag32 (-2l));
  assert_equal  0L (zigzag64 0L);
  assert_equal  1L (zigzag64 (-1L));
  assert_equal  2L (zigzag64 1L);
  assert_equal  3L (zigzag64 (-2L));
  if Sys.word_size = 32 then
    assert_raises (Reader.Error Overflow) (fun () -> Reader.int_of_int32 0xffffffffl);
  assert_raises (Reader.Error Overflow) (fun () -> Reader.int_of_int64 0xffffffffffffffffL);
  ()

let test_reader ctxt =
  let r = Reader.of_string "\x01" in
  assert_equal ~printer:Int64.to_string 1L (Reader.varint r);
  let r = Reader.of_string "\xac\x02" in
  assert_equal ~printer:Int64.to_string 300L (Reader.varint r);
  let r = Reader.of_string "\x01\x02\x03\x04" in
  assert_equal ~printer:Int32.to_string 0x04030201l (Reader.int32 r);
  let r = Reader.of_string "\x01\x02\x03\x04\x05\x06\x07\x08" in
  assert_equal ~printer:Int64.to_string 0x0807060504030201L (Reader.int64 r);
  let r = Reader.of_string "\x03abc" in
  assert_equal ~printer:(fun x -> x) "abc" (Reader.bytes r);
  assert_raises (Reader.Error Incomplete) (fun () -> Reader.varint r);
  let r  = Reader.of_string "\x02\xac\x02" in
  let r' = Reader.nested r in
  assert_equal ~printer:Int64.to_string 300L (Reader.varint r');
  assert_raises (Reader.Error Incomplete) (fun () -> Reader.varint r');
  assert_raises (Reader.Error Incomplete) (fun () -> Reader.varint r);
  let r  = Reader.of_string "\x08\x11\x1a\x25" in
  assert_equal (Some (1, Varint)) (Reader.key r);
  assert_equal (Some (2, Bits64)) (Reader.key r);
  assert_equal (Some (3, Bytes))  (Reader.key r);
  assert_equal (Some (4, Bits32)) (Reader.key r);
  assert_equal None (Reader.key r);
  ()

let suite = "Test Protobuf" >::: [
    "test_xforms" >:: test_xforms;
    "test_reader" >:: test_reader;
  ]
