open OUnit2
open Protobuf

let test_reader ctxt =
  let r = Wire.reader_of_string "\x01" in
  assert_equal ~printer:Int64.to_string 1L (Wire.read_varint r);
  let r = Wire.reader_of_string "\xac\x02" in
  assert_equal ~printer:Int64.to_string 300L (Wire.read_varint r);
  let r = Wire.reader_of_string "\xac\x02\xff\xff\xff\xff\xff\xff\xff\xff\x7f" in
  assert_equal ~printer:string_of_int 300 (Wire.read_smallint r);
  assert_raises (Decoding_error Overflow) (fun () -> Wire.read_smallint r);
  let r = Wire.reader_of_string "\x01\x02\x03\x04" in
  assert_equal ~printer:Int32.to_string 0x04030201l (Wire.read_int32 r);
  let r = Wire.reader_of_string "\x01\x02\x03\x04\x05\x06\x07\x08" in
  assert_equal ~printer:Int64.to_string 0x0807060504030201L (Wire.read_int64 r);
  let r = Wire.reader_of_string "\x03abc" in
  assert_equal ~printer:(fun x -> x) "abc" (Wire.read_bytes r);
  assert_raises (Decoding_error Incomplete) (fun () -> Wire.read_varint r);
  let r  = Wire.reader_of_string "\x02\xac\x02" in
  let r' = Wire.read_nested r in
  assert_equal ~printer:string_of_int 300 (Wire.read_smallint r');
  assert_raises (Decoding_error Incomplete) (fun () -> Wire.read_varint r');
  assert_raises (Decoding_error Incomplete) (fun () -> Wire.read_varint r);
  let r  = Wire.reader_of_string "\x08\x11\x1a\x25" in
  assert_equal (Some (1, Varint)) (Wire.read_key r);
  assert_equal (Some (2, Bits64)) (Wire.read_key r);
  assert_equal (Some (3, Bytes))  (Wire.read_key r);
  assert_equal (Some (4, Bits32)) (Wire.read_key r);
  assert_equal None (Wire.read_key r);
  ()

let suite = "Test Protobuf" >::: [
    "test_reader" >:: test_reader;
  ]
