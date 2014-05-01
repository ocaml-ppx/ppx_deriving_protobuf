open OUnit2
open Protobuf

let test_decoder ctxt =
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
  let d = Decoder.of_string "\x00" in
  assert_equal ~printer:Int64.to_string 0L (Decoder.zigzag d);
  let d = Decoder.of_string "\x01" in
  assert_equal ~printer:Int64.to_string (-1L) (Decoder.zigzag d);
  let d = Decoder.of_string "\x02" in
  assert_equal ~printer:Int64.to_string 1L (Decoder.zigzag d);
  let d = Decoder.of_string "\x03" in
  assert_equal ~printer:Int64.to_string (-2L) (Decoder.zigzag d);
  ()

let test_encoder ctxt =
  let printer s = Printf.sprintf "%S" s in
  let e = Encoder.create () in
  Encoder.varint 1L e;
  assert_equal ~printer "\x01" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.varint 300L e;
  assert_equal ~printer "\xac\x02" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.bits32 0x04030201l e;
  assert_equal ~printer "\x01\x02\x03\x04" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.bits64 0x0807060504030201L e;
  assert_equal ~printer "\x01\x02\x03\x04\x05\x06\x07\x08" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.bytes "abc" e;
  assert_equal ~printer "\x03abc" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.nested (Encoder.varint 300L) e;
  assert_equal ~printer "\x02\xac\x02" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.key (1, Varint) e;
  Encoder.key (2, Bits64) e;
  Encoder.key (3, Bytes) e;
  Encoder.key (4, Bits32) e;
  assert_equal ~printer "\x08\x11\x1a\x25" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.zigzag 0L e;
  assert_equal ~printer "\x00" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.zigzag (-1L) e;
  assert_equal ~printer "\x01" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.zigzag 1L e;
  assert_equal ~printer "\x02" (Encoder.to_string e);
  let e = Encoder.create () in
  Encoder.zigzag (-2L) e;
  assert_equal ~printer "\x03" (Encoder.to_string e);
  ()

let test_overflow ctxt =
  if Sys.word_size = 32 then
    assert_raises Decoder.(Failure (Overflow ""))
                  (fun () -> Decoder.int_of_int32 "" 0xffffffffl)
  else
    assert_equal (-1) (Decoder.int_of_int32 "" 0xffffffffl);
  assert_raises Decoder.(Failure (Overflow ""))
                (fun () -> Decoder.int_of_int64 "" 0xffffffffffffffffL);
  assert_raises Decoder.(Failure (Overflow ""))
                (fun () -> Decoder.int32_of_int64 "" 0x1ffffffffL);
  assert_raises Decoder.(Failure (Overflow ""))
                (fun () -> Decoder.bool_of_int64 "" 2L);
  if Sys.word_size = 64 then
    assert_raises Encoder.(Failure (Overflow ""))
                  (fun () -> Encoder.int32_of_int "" (2 lsl 33));
  assert_raises Encoder.(Failure (Overflow ""))
                (fun () -> Encoder.int32_of_int64 "" 0x1ffffffffL);
  ()

let suite = "Test wire format" >::: [
    "test_decoder"  >:: test_decoder;
    "test_encoder"  >:: test_encoder;
    "test_overflow" >:: test_overflow;
  ]
