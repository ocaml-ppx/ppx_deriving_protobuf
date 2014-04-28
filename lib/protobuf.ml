type payload_kind =
| Varint
| Bits32
| Bits64
| Bytes

module Decoder = struct
  type error =
  | Incomplete
  | Overflow
  | Malformed_field
  | Unexpected_payload  of string * payload_kind
  | Missing_field       of string

  let error_to_string e =
    match e with
    | Incomplete      -> "Incomplete"
    | Overflow        -> "Overflow"
    | Malformed_field -> "Malformed_field"
    | Unexpected_payload (field, kind) ->
      let kind' =
        match kind with
        | Varint -> "Varint"
        | Bits32 -> "Bits32"
        | Bits64 -> "Bits64"
        | Bytes  -> "Bytes"
      in
      Printf.sprintf "Unexpected_payload(%S, %s)" field kind'
    | Missing_field field ->
      Printf.sprintf "Missing_field(%S)" field

  exception Failure of error

  let () =
    Printexc.register_printer (fun exn ->
      match exn with
      | Failure e -> Some (Printf.sprintf "Protobuf.Reader.Failure(%s)" (error_to_string e))
      | _         -> None)

  type t = {
            source : string;
            limit  : int;
    mutable offset : int;
  }

  let int_of_int32 v =
    if Sys.word_size = 32 && (Int32.shift_right v 31) <> Int32.zero then
      raise (Failure Overflow);
    Int32.to_int v

  let int_of_int64 v =
    if (Int64.shift_right v 63) <> Int64.zero then
      raise (Failure Overflow);
    Int64.to_int v

  let of_string source =
    { source; offset = 0; limit = String.length source; }

  let byte d =
    if d.offset >= d.limit then
      raise (Failure Incomplete);
    let byte = int_of_char d.source.[d.offset] in
    d.offset <- d.offset + 1;
    byte

  let bool d =
    let b = byte d in
    match b with
    | 0 -> false
    | 1 -> true
    | _ -> raise (Failure Overflow)

  let varint d =
    let rec read s =
      let b = byte d in
      if b land 0x80 <> 0
      then Int64.(logor (shift_left (logand (of_int b) 0x7fL) s) (read (s + 7)))
      else Int64.(shift_left (of_int b) s)
    in
    read 0

  let zigzag d =
    let v = varint d in
    Int64.(logxor (shift_right v 1) (neg (logand v Int64.one)))

  let bits32 d =
    let b4 = byte d in
    let b3 = byte d in
    let b2 = byte d in
    let b1 = byte d in
    Int32.(add (shift_left (of_int b1) 24)
           (add (shift_left (of_int b2) 16)
            (add (shift_left (of_int b3) 8)
             (of_int b4))))

  let bits64 d =
    let b8 = byte d in
    let b7 = byte d in
    let b6 = byte d in
    let b5 = byte d in
    let b4 = byte d in
    let b3 = byte d in
    let b2 = byte d in
    let b1 = byte d in
    Int64.(add (shift_left (of_int b1) 56)
           (add (shift_left (of_int b2) 48)
            (add (shift_left (of_int b3) 40)
             (add (shift_left (of_int b4) 32)
              (add (shift_left (of_int b5) 24)
               (add (shift_left (of_int b6) 16)
                (add (shift_left (of_int b7) 8)
                 (of_int b8))))))))

  let smallint d =
    int_of_int64 (varint d)

  let bytes d =
    let len = smallint d in
    if d.offset + len > d.limit then
      raise (Failure Incomplete);
    let str = String.sub d.source d.offset len in
    d.offset <- d.offset + len;
    str

  let nested d =
    let len = smallint d in
    if d.offset + len > d.limit then
      raise (Failure Incomplete);
    let d' = { d with limit = d.offset + len; } in
    d.offset <- d.offset + len;
    d'

  let key d =
    if d.offset = d.limit
    then None
    else
      let prefix  = smallint d in
      let key, ty = prefix lsr 3, prefix land 0x7 in
      match ty with
      | 0 -> Some (key, Varint)
      | 1 -> Some (key, Bits64)
      | 2 -> Some (key, Bytes)
      | 5 -> Some (key, Bits32)
      | _ -> raise (Failure Malformed_field)

  let skip d kind =
    let skip_len n =
      if d.offset + n > d.limit then
        raise (Failure Incomplete);
      d.offset <- d.offset + d.limit
    in
    let rec skip_varint () =
      let b = byte d in
      if b land 0x80 <> 0 then skip_varint () else ()
    in
    match kind with
    | Bits32 -> skip_len 4
    | Bits64 -> skip_len 8
    | Bytes  -> skip_len (smallint d)
    | Varint -> skip_varint ()
end
