type payload_kind =
| Varint
| Bits32
| Bits64
| Bytes

module Reader = struct
  type t = {
            source : string;
            limit  : int;
    mutable offset : int;
  }

  type error =
  | Incomplete
  | Overflow
  | Malformed_field
  | Unexpected_payload  of string * payload_kind
  | Missing_field       of string

  exception Error of error

  let () =
    Printexc.register_printer (fun exn ->
      match exn with
      | Error reason ->
        let reason' =
          match reason with
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
        in
        Some (Printf.sprintf "Protobuf.Reader.Error(%s)" reason')
      | _ -> None)

  let int_of_int32 v =
    if Sys.word_size = 32 && (Int32.shift_right v 31) <> Int32.zero then
      raise (Error Overflow);
    Int32.to_int v

  let int_of_int64 v =
    if (Int64.shift_right v 63) <> Int64.zero then
      raise (Error Overflow);
    Int64.to_int v

  let of_string source =
    { source; offset = 0; limit = String.length source; }

  let byte r =
    if r.offset >= r.limit then
      raise (Error Incomplete);
    let byte = int_of_char r.source.[r.offset] in
    r.offset <- r.offset + 1;
    byte

  let bool r =
    let b = byte r in
    match b with
    | 0 -> false
    | 1 -> true
    | _ -> raise (Error Overflow)

  let varint r =
    let rec read s =
      let b = byte r in
      if b land 0x80 <> 0
      then Int64.(logor (shift_left (logand (of_int b) 0x7fL) s) (read (s + 7)))
      else Int64.(shift_left (of_int b) s)
    in
    read 0

  let zigzag r =
    let v = varint r in
    Int64.(logxor (shift_right v 1) (neg (logand v Int64.one)))

  let bits32 r =
    let b4 = byte r in
    let b3 = byte r in
    let b2 = byte r in
    let b1 = byte r in
    Int32.(add (shift_left (of_int b1) 24)
           (add (shift_left (of_int b2) 16)
            (add (shift_left (of_int b3) 8)
             (of_int b4))))

  let bits64 r =
    let b8 = byte r in
    let b7 = byte r in
    let b6 = byte r in
    let b5 = byte r in
    let b4 = byte r in
    let b3 = byte r in
    let b2 = byte r in
    let b1 = byte r in
    Int64.(add (shift_left (of_int b1) 56)
           (add (shift_left (of_int b2) 48)
            (add (shift_left (of_int b3) 40)
             (add (shift_left (of_int b4) 32)
              (add (shift_left (of_int b5) 24)
               (add (shift_left (of_int b6) 16)
                (add (shift_left (of_int b7) 8)
                 (of_int b8))))))))

  let smallint r =
    int_of_int64 (varint r)

  let bytes r =
    let len = smallint r in
    if r.offset + len > r.limit then
      raise (Error Incomplete);
    let str = String.sub r.source r.offset len in
    r.offset <- r.offset + len;
    str

  let nested r =
    let len = smallint r in
    if r.offset + len > r.limit then
      raise (Error Incomplete);
    let r' = { r with limit = r.offset + len; } in
    r.offset <- r.offset + len;
    r'

  let key r =
    if r.offset = r.limit
    then None
    else
      let prefix  = smallint r in
      let key, ty = prefix lsr 3, prefix land 0x7 in
      match ty with
      | 0 -> Some (key, Varint)
      | 1 -> Some (key, Bits64)
      | 2 -> Some (key, Bytes)
      | 5 -> Some (key, Bits32)
      | _ -> raise (Error Malformed_field)

  let skip r kind =
    let skip_len n =
      if r.offset + n > r.limit then
        raise (Error Incomplete);
      r.offset <- r.offset + r.limit
    in
    let rec skip_varint () =
      let b = byte r in
      if b land 0x80 <> 0 then skip_varint () else ()
    in
    match kind with
    | Bits32 -> skip_len 4
    | Bits64 -> skip_len 8
    | Bytes  -> skip_len (smallint r)
    | Varint -> skip_varint ()
end
