open Longident
open Location
open Asttypes
open Parsetree
open Ast_mapper
open Ast_helper
open Ast_convenience

type pb_encoding =
| Pbe_varint
| Pbe_zigzag
| Pbe_bits32
| Pbe_bits64
| Pbe_bytes
| Pbe_packed of pb_encoding
and pb_type =
| Pbt_bool
| Pbt_int
| Pbt_int32
| Pbt_int64
| Pbt_uint32
| Pbt_uint64
| Pbt_float
| Pbt_string
| Pbt_imm     of core_type
| Pbt_variant of (int * string) list
| Pbt_nested  of core_type list * Longident.t
| Pbt_poly    of string
and pb_kind =
| Pbk_required
| Pbk_optional
| Pbk_repeated
and pb_field = {
  pbf_name    : string;
  pbf_path    : string;
  pbf_key     : int;
  pbf_enc     : pb_encoding;
  pbf_type    : pb_type;
  pbf_kind    : pb_kind;
  pbf_default : expression option;
  pbf_loc     : Location.t;
}

type error =
| Pberr_attr_syntax   of Location.t * [ `Key | `Encoding | `Bare | `Default | `Packed ]
| Pberr_wrong_attr    of attribute
| Pberr_no_key        of Location.t
| Pberr_key_invalid   of Location.t * int
| Pberr_key_duplicate of int * Location.t * Location.t
| Pberr_abstract      of type_declaration
| Pberr_open          of type_declaration
| Pberr_wrong_ty      of core_type
| Pberr_wrong_tparm   of core_type
| Pberr_no_conversion of Location.t * pb_type * pb_encoding
| Pberr_packed_bytes  of Location.t

exception Error of error

let with_formatter f =
  let buf = Buffer.create 16 in
  let fmt = Format.formatter_of_buffer buf in
  f fmt;
  Format.pp_print_flush fmt ();
  Buffer.contents buf

let string_of_core_type ptyp =
  (with_formatter (fun fmt ->
    (* Get rid of visual noise *)
    Pprintast.core_type fmt { ptyp with ptyp_attributes = [] }))

let string_of_lident lid =
  String.concat "." (Longident.flatten lid)

let rec string_of_pb_encoding enc =
  match enc with
  | Pbe_varint -> "varint"
  | Pbe_zigzag -> "zigzag"
  | Pbe_bits32 -> "bits32"
  | Pbe_bits64 -> "bits64"
  | Pbe_bytes  -> "bytes"
  | Pbe_packed enc -> "packed " ^ (string_of_pb_encoding enc)

let pb_encoding_of_string str =
  match str with
  | "varint" -> Pbe_varint
  | "zigzag" -> Pbe_zigzag
  | "bits32" -> Pbe_bits32
  | "bits64" -> Pbe_bits64
  | "bytes"  -> Pbe_bytes
  | _ -> raise Not_found

let rec string_of_pb_type kind =
  match kind with
  | Pbt_bool   -> "bool"
  | Pbt_int    -> "int"
  | Pbt_int32  -> "Int32.t"
  | Pbt_int64  -> "Int64.t"
  | Pbt_uint32 -> "Uint32.t"
  | Pbt_uint64 -> "Uint64.t"
  | Pbt_float  -> "float"
  | Pbt_string -> "string"
  | Pbt_imm ptyp ->
    string_of_core_type ptyp
  | Pbt_variant constrs ->
    String.concat " | " (List.map snd constrs)
  | Pbt_nested (args, lid) ->
    begin match args with
    | []   -> ""
    | args -> Printf.sprintf "(%s) " (String.concat ", " (List.map string_of_core_type args))
    end ^ string_of_lident lid
  | Pbt_poly var -> "'" ^ var

let string_payload_kind_of_pb_encoding enc =
  "Protobuf." ^
  match enc with
  | Pbe_varint | Pbe_zigzag   -> "Varint"
  | Pbe_bytes  | Pbe_packed _ -> "Bytes"
  | Pbe_bits32 -> "Bits32"
  | Pbe_bits64 -> "Bits64"

let describe_error error =
  match error with
  | Pberr_attr_syntax (loc, attr) ->
    let name, expectation =
      match attr with
      | `Key      -> "key", "a number, e.g. [@key 1]"
      | `Encoding -> "encoding", "one of: bool, varint, zigzag, bits32, bits64, " ^
                                 "bytes, e.g. [@encoding varint]"
      | `Bare     -> "bare", "[@bare]"
      | `Default  -> "default", "an expression, e.g. [@default \"foo\"]"
      | `Packed   -> "packed", "[@packed]"
    in
    errorf ~loc "@%s attribute syntax is invalid: expected %s" name expectation
  | Pberr_wrong_attr ({ txt; loc }, _) ->
    errorf ~loc "Attribute @%s is not recognized here" txt
  | Pberr_no_key loc ->
    errorf ~loc "Type specification must include a key attribute, e.g. int [@key 42]"
  | Pberr_key_invalid (loc, key) ->
    if key >= 19000 && key <= 19999 then
      errorf ~loc "Key %d is in reserved range [19000:19999]" key
    else
      errorf ~loc "Key %d is outside of valid range [1:0x1fffffff]" key
  | Pberr_key_duplicate (key, loc1, loc2) ->
    errorf ~loc:loc1 "Key %d is already used" key
                 ~sub:[errorf ~loc:loc2 "Initially defined here"]
  | Pberr_abstract { ptype_loc = loc } ->
    errorf ~loc "Abstract types are not supported"
  | Pberr_open { ptype_loc = loc } ->
    errorf ~loc "Open types are not supported"
  | Pberr_wrong_ty ({ ptyp_loc = loc } as ptyp) ->
    errorf ~loc "Type %s does not have a Protobuf mapping" (string_of_core_type ptyp)
  | Pberr_wrong_tparm ({ ptyp_loc = loc } as ptyp) ->
    errorf ~loc "Type %s cannot be used as a type parameter" (string_of_core_type ptyp)
  | Pberr_no_conversion (loc, kind, enc) ->
    errorf ~loc "\"%s\" is not a valid representation for %s"
                      (string_of_pb_encoding enc) (string_of_pb_type kind)
  | Pberr_packed_bytes loc ->
    errorf ~loc "Only fields with varint, bits32 or bits64 encoding may be packed"

let () =
  Location.register_error_of_exn (fun exn ->
    match exn with
    | Error err -> Some (describe_error err)
    | _ -> None)

let mangle_lid ?(suffix="") lid =
  match lid with
  | Lident s    -> Lident (s ^ suffix)
  | Ldot (p, s) -> Ldot (p, s ^ suffix)
  | Lapply _    -> assert false

let module_name () =
  match !Location.input_name with
  | "//toplevel//" -> "(toplevel)"
  | filename -> String.capitalize (Filename.(basename (chop_suffix filename ".ml")))

let find_attr name attrs =
  let prefixed_name = "protobuf." ^ name in
  List.find (fun ({ txt }, _) -> txt = name || txt = prefixed_name) attrs

let pb_key_of_attrs attrs =
  match find_attr "key" attrs with
  | { loc }, PStr [{ pstr_desc = Pstr_eval ({
                        pexp_desc = Pexp_constant (Const_int key) }, _) }] ->
    if key < 1 || key > 0x1fffffff || (key >= 19000 && key <= 19999) then
      raise (Error (Pberr_key_invalid (loc, key)));
    key
  | { loc }, _ -> raise (Error (Pberr_attr_syntax (loc, `Key)))

let pb_encoding_of_attrs attrs =
  match find_attr "encoding" attrs with
  | _, PStr [{ pstr_desc = Pstr_eval ({
                pexp_desc = Pexp_ident { txt = Lident kind; loc } }, _) }] ->
    begin try
      pb_encoding_of_string kind
    with Not_found ->
      raise (Error (Pberr_attr_syntax (loc, `Encoding)))
    end
  | { loc }, _ -> raise (Error (Pberr_attr_syntax (loc, `Encoding)))

let bare_of_attrs attrs =
  match find_attr "bare" attrs with
  | _, PStr [] -> ()
  | { loc }, _ -> raise (Error (Pberr_attr_syntax (loc, `Bare)))

let default_of_attrs attrs =
  match find_attr "default" attrs with
  | _, PStr [{ pstr_desc = Pstr_eval (expr, _) }] -> expr
  | { loc }, _ -> raise (Error (Pberr_attr_syntax (loc, `Default)))

let packed_of_attrs attrs =
  match find_attr "packed" attrs with
  | _, PStr [] -> ()
  | { loc }, _ -> raise (Error (Pberr_attr_syntax (loc, `Packed)))

let all_attrs = ["key"; "encoding"; "bare"; "default"; "packed"]
let check_attrs allow attrs =
  let forbid = List.filter (fun attr -> not (List.exists ((=) attr) allow)) all_attrs in
  let forbid = forbid @ List.map (fun attr -> "protobuf." ^ attr) forbid in
  forbid |> List.iter (fun attr ->
    try  let attr = List.find (fun ({ txt }, _) -> txt = attr) attrs in
         raise (Error (Pberr_wrong_attr attr))
    with Not_found -> ())

let fields_of_ptype base_path ptype =
  let rec field_of_ptyp pbf_name pbf_path pbf_key pbf_kind ptyp =
    match ptyp with
    | [%type: [%t? arg] option] ->
      check_attrs ["key"; "encoding"] ptyp.ptyp_attributes;
      begin match pbf_kind with
      | Pbk_required -> field_of_ptyp pbf_name pbf_path pbf_key Pbk_optional arg
      | _ -> raise (Error (Pberr_wrong_ty ptyp))
      end
    | [%type: [%t? arg] array] | [%type: [%t? arg] list] ->
      check_attrs ["key"; "encoding"; "packed"] ptyp.ptyp_attributes;
      begin match pbf_kind with
      | Pbk_required ->
        let { pbf_enc } as field = field_of_ptyp pbf_name pbf_path pbf_key Pbk_repeated arg in
        let pbf_enc =
          try  packed_of_attrs ptyp.ptyp_attributes; Pbe_packed pbf_enc
          with Not_found -> pbf_enc
        in
        if pbf_enc = Pbe_packed Pbe_bytes then
          raise (Error (Pberr_packed_bytes ptyp.ptyp_loc));
        { field with pbf_enc }
      | _ -> raise (Error (Pberr_wrong_ty ptyp))
      end
    | { ptyp_desc = (Ptyp_tuple _ | Ptyp_variant _ | Ptyp_var _) as desc;
        ptyp_attributes = attrs; ptyp_loc; } ->
      begin match desc with
      | Ptyp_variant _ -> check_attrs ["key"; "encoding"; "bare"; "default"; "packed"] attrs
      | Ptyp_tuple _   -> check_attrs ["key"; "encoding"; "default"] attrs
      | _              -> check_attrs ["key"; "encoding"] attrs
      end;
      let pbf_key =
        try  pb_key_of_attrs attrs
        with Not_found ->
          match pbf_key with
          | Some k -> k
          | None -> raise (Error (Pberr_no_key ptyp_loc))
      in
      let pbf_enc, pbf_type =
        match desc with
        | Ptyp_variant _ ->
          let pbf_enc =
            try  bare_of_attrs attrs; Pbe_varint
            with Not_found -> Pbe_bytes
          in pbf_enc, Pbt_imm ptyp
        | Ptyp_tuple _   -> Pbe_bytes, Pbt_imm ptyp
        | Ptyp_var var   -> Pbe_bytes, Pbt_poly var
        | _ -> assert false
      in
      { pbf_name; pbf_key; pbf_kind; pbf_path; pbf_type; pbf_enc;
        pbf_loc     = ptyp_loc;
        pbf_default = try Some (default_of_attrs attrs) with Not_found -> None; }
    | { ptyp_desc = Ptyp_constr ({ txt = lid }, args); ptyp_attributes = attrs; ptyp_loc; } ->
      let pbf_type =
        match args, lid with
        | [], Lident "bool"   -> Pbt_bool
        | [], Lident "int"    -> Pbt_int
        | [], Lident "float"  -> Pbt_float
        | [], Lident "string" -> Pbt_string
        | [], (Lident "int32"  | Ldot (Lident "Int32", "t"))  -> Pbt_int32
        | [], (Lident "int64"  | Ldot (Lident "Int64", "t"))  -> Pbt_int64
        | [], (Lident "uint32" | Ldot (Lident "Uint32", "t")) -> Pbt_uint32
        | [], (Lident "uint64" | Ldot (Lident "Uint64", "t")) -> Pbt_uint64
        | _,  lident -> Pbt_nested (args, lident)
      in
      begin match pbf_type with
      | Pbt_nested _ -> check_attrs ["key"; "encoding"; "bare"; "default"] attrs
      | _            -> check_attrs ["key"; "encoding"; "default"; "packed"] attrs
      end;
      let pbf_key =
        try  pb_key_of_attrs attrs
        with Not_found ->
          match pbf_key with
          | Some k -> k
          | None -> raise (Error (Pberr_no_key ptyp_loc))
      in
      let pbf_enc =
        try  pb_encoding_of_attrs attrs
        with Not_found ->
          match pbf_type with
          | Pbt_float  -> Pbe_bits64
          | Pbt_bool   | Pbt_int     -> Pbe_varint
          | Pbt_int32  | Pbt_uint32  -> Pbe_bits32
          | Pbt_int64  | Pbt_uint64  -> Pbe_bits64
          | Pbt_string | Pbt_imm _   | Pbt_poly _ -> Pbe_bytes
          | Pbt_nested _ ->
            begin
              try  bare_of_attrs attrs; Pbe_varint
              with Not_found -> Pbe_bytes
            end
          | Pbt_variant _ -> assert false
      in
      begin match pbf_type, pbf_enc with
      | Pbt_bool, Pbe_varint
      | (Pbt_int | Pbt_int32 | Pbt_int64 | Pbt_uint32 | Pbt_uint64),
        (Pbe_varint | Pbe_zigzag | Pbe_bits32 | Pbe_bits64)
      | Pbt_float, (Pbe_bits32 | Pbe_bits64)
      | Pbt_string, Pbe_bytes
      | Pbt_nested _, (Pbe_bytes | Pbe_varint) ->
        { pbf_name; pbf_key; pbf_enc; pbf_type; pbf_kind; pbf_path;
          pbf_loc     = ptyp_loc;
          pbf_default = try Some (default_of_attrs attrs) with Not_found -> None }
      | _ ->
        raise (Error (Pberr_no_conversion (ptyp_loc, pbf_type, pbf_enc)))
      end
    | { ptyp_desc = Ptyp_alias (ptyp', _) } ->
      field_of_ptyp pbf_name pbf_path pbf_key pbf_kind ptyp'
    | ptyp -> raise (Error (Pberr_wrong_ty ptyp))
  in
  let fields_of_variant loc constrs =
    let constrs' =
      constrs |> List.map (fun ((name, args, attrs, loc) as pcd) ->
        let key =
          try  pb_key_of_attrs attrs
          with Not_found -> raise (Error (Pberr_no_key loc))
        in key, pcd)
    in
    constrs' |> List.iter (fun (key, (_, _, _, loc) as pcd) ->
      constrs' |> List.iter (fun (key', (_, _, _, loc') as pcd') ->
        if pcd != pcd' && key = key' then
          raise (Error (Pberr_key_duplicate (key, loc', loc)))));
    { pbf_name    = "variant";
      pbf_path    = base_path;
      pbf_key     = 1;
      pbf_enc     = Pbe_varint;
      pbf_type    = Pbt_variant (constrs' |> List.map (fun (key, (name, _, _, _)) -> key, name));
      pbf_kind    = Pbk_required;
      pbf_loc     = loc;
      pbf_default = None; } ::
    (constrs |> ExtList.List.filter_map (fun (name, args, attrs, loc) ->
      let ptyp =
        match args with
        | []    -> None
        | [arg] -> Some arg
        | args  -> Some (Typ.tuple args)
      in
      ptyp |> Option.map (fun ptyp ->
        check_attrs ["key"] attrs;
        let key = (pb_key_of_attrs attrs) + 1 in
        field_of_ptyp (Printf.sprintf "constr_%s" name)
                      (Printf.sprintf "%s.%s" base_path name)
                      (Some key) Pbk_required ptyp)))
  in
  let fields =
    match ptype with
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some { ptyp_desc = Ptyp_tuple ptyps } } ->
      ptyps |> List.mapi (fun i ptyp ->
        field_of_ptyp (Printf.sprintf "elem_%d" i) (Printf.sprintf "%s/%d" base_path i)
                      (Some (i + 1)) Pbk_required ptyp)
    | { ptype_kind = Ptype_abstract;
        ptype_manifest = Some ({ ptyp_desc = Ptyp_variant (rows, _, _); ptyp_loc } as ptyp);
        ptype_loc; } ->
      rows |> List.map (fun row_field ->
        match row_field with
        | Rtag (name, attrs, _, [])  -> (name, [], attrs, ptyp_loc)
        | Rtag (name, attrs, _, [a]) -> (name, [a], attrs, ptyp_loc)
        | _ -> raise (Error (Pberr_wrong_ty ptyp))) |> fields_of_variant ptype_loc
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some ptyp } ->
      [field_of_ptyp "alias" base_path (Some 1) Pbk_required ptyp]
    | { ptype_kind = Ptype_abstract; ptype_manifest = None } ->
      raise (Error (Pberr_abstract ptype))
    | { ptype_kind = Ptype_open } ->
      raise (Error (Pberr_open ptype))
    | { ptype_kind = Ptype_record fields } ->
      fields |> List.mapi (fun i { pld_name = { txt = name }; pld_type; pld_attributes; } ->
        check_attrs [] pld_attributes;
        field_of_ptyp ("field_" ^ name) (Printf.sprintf "%s.%s" base_path name)
                      None Pbk_required pld_type)
    | { ptype_kind = Ptype_variant constrs; ptype_loc } ->
      constrs |> List.map (fun { pcd_name = { txt = name }; pcd_args; pcd_attributes; pcd_loc; } ->
        (name, pcd_args, pcd_attributes, pcd_loc)) |> fields_of_variant ptype_loc
  in
  fields |> List.iter (fun field ->
    fields |> List.iter (fun field' ->
      if field != field' && field.pbf_key = field'.pbf_key then
        raise (Error (Pberr_key_duplicate (field.pbf_key, field'.pbf_loc, field.pbf_loc)))));
  fields |> List.sort (fun { pbf_key = a } { pbf_key = b } -> compare a b)

let rec mk_poly tvars k =
  match tvars with
  | ({ ptyp_desc = Ptyp_var var }, _) :: rest ->
    [%expr fun [%p pvar ("poly_" ^ var)] -> [%e mk_poly rest k]]
  | (_, _) :: rest -> mk_poly rest k
  | [] -> k

let derive_reader_bare fields ptype =
  let mk_variant mk_constr constrs =
    let rec mk_variant_cases constrs =
      match constrs with
      | (name, attrs) :: rest ->
        (Exp.case (Pat.constant (Const_int64
                    (Int64.of_int (pb_key_of_attrs attrs))))
                  (mk_constr name)) :: mk_variant_cases rest
      | [] ->
        let name = (module_name ()) ^ "." ^ ptype.ptype_name.txt in
        [Exp.case [%pat? _] [%expr raise Protobuf.Decoder.
                              (Failure (Malformed_variant [%e str name]))]]
    in
    let matcher =
      Exp.match_ [%expr Protobuf.Decoder.varint decoder]
                 (mk_variant_cases constrs) in
    Some (Vb.mk (pvar (ptype.ptype_name.txt ^ "_from_protobuf_bare"))
                [%expr fun decoder -> [%e matcher]])
  in
  match ptype with
  | { ptype_kind = Ptype_variant constrs } when
      List.for_all (fun { pcd_args } -> pcd_args = []) constrs ->
    constrs |> List.map (fun { pcd_name = { txt = name }; pcd_attributes } ->
                  name, pcd_attributes) |> mk_variant (fun name -> constr name [])
  | { ptype_kind = Ptype_abstract;
      ptype_manifest = Some { ptyp_desc = Ptyp_variant (rows, _, _) } } when
        List.for_all (fun row_field ->
          match row_field with Rtag (_, _, _, []) -> true | _ -> false) rows ->
    rows |> List.map (fun row_field ->
      match row_field with
      | Rtag (name, attrs, _, []) -> (name, attrs)
      | _ -> assert false) |> mk_variant (fun name -> Exp.variant name None)
  | _ -> None

let rec derive_reader fields ptype =
  let rec mk_imm_readers fields k =
    match fields with
    | { pbf_type = Pbt_imm ty; pbf_name; pbf_path; } :: rest ->
      (* Manufacture a structure just for this immediate *)
      let ptype = Type.mk ~manifest:ty (mkloc ("_" ^ pbf_name) !default_loc) in
      Exp.let_ Nonrecursive
               ((derive_reader (fields_of_ptype pbf_path ptype) ptype) ::
                (Option.map_default (fun x -> [x]) [] (derive_reader_bare fields ptype)))
               (mk_imm_readers rest k)
    | _ :: rest -> mk_imm_readers rest k
    | [] -> k
  in
  let rec mk_cells fields k =
    match fields with
    | { pbf_kind = (Pbk_required | Pbk_optional) } as field :: rest ->
      [%expr let [%p pvar field.pbf_name] = ref None in [%e mk_cells rest k]]
    | { pbf_kind = Pbk_repeated } as field :: rest ->
      [%expr let [%p pvar field.pbf_name] = ref [] in [%e mk_cells rest k]]
    | [] -> k
  in
  let rec mk_reader ({ pbf_name; pbf_path; pbf_enc; pbf_type; } as field) =
    let value =
      let ident = Exp.ident (lid ("Protobuf.Decoder." ^ (string_of_pb_encoding pbf_enc))) in
      [%expr [%e ident] decoder]
    in
    match pbf_type, pbf_enc with
    (* bool *)
    | Pbt_bool, Pbe_varint ->
      [%expr Protobuf.Decoder.bool_of_int64 [%e str pbf_path] [%e value]]
    (* int *)
    | Pbt_int, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Protobuf.Decoder.int_of_int64 [%e str pbf_path] [%e value]]
    | Pbt_int, Pbe_bits32 ->
      [%expr Protobuf.Decoder.int_of_int32 [%e str pbf_path] [%e value]]
    (* int32 *)
    | Pbt_int32, Pbe_bits32 -> value
    | Pbt_int32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Protobuf.Decoder.int32_of_int64 [%e str pbf_path] [%e value]]
    (* int64 *)
    | Pbt_int64, (Pbe_varint | Pbe_zigzag | Pbe_bits64) -> value
    | Pbt_int64, Pbe_bits32 ->
      [%expr Int64.of_int32 [%e value]]
    (* uint32 *)
    | Pbt_uint32, Pbe_bits32 ->
      [%expr Uint32.of_int32 [%e value]]
    | Pbt_uint32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Uint32.of_int32 (Protobuf.Decoder.int32_of_int64 [%e str pbf_path] [%e value])]
    (* uint64 *)
    | Pbt_uint64, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Uint64.of_int64 [%e value]]
    | Pbt_uint64, Pbe_bits32 ->
      [%expr Uint64.of_int32 [%e value]]
    (* float *)
    | Pbt_float, Pbe_bits32 ->
      [%expr Int32.float_of_bits [%e value]]
    | Pbt_float, Pbe_bits64 ->
      [%expr Int64.float_of_bits [%e value]]
    (* string *)
    | Pbt_string, Pbe_bytes -> value
    (* variant *)
    | Pbt_variant _, Pbe_varint -> value
    (* nested *)
    | Pbt_nested (args, lid), Pbe_bytes ->
      let ident = Exp.ident (mkloc (mangle_lid ~suffix:"_from_protobuf" lid) !default_loc) in
      let args' = args |> List.map (fun ptyp ->
        match ptyp with
        | { ptyp_desc = Ptyp_var tvar } -> evar ("poly_" ^ tvar)
        | _ -> raise (Error (Pberr_wrong_tparm ptyp)))
      in
      app ident (args' @ [[%expr Protobuf.Decoder.nested decoder]])
    | Pbt_nested ([], lid), Pbe_varint -> (* bare enum *)
      let ident = Exp.ident (mkloc (mangle_lid ~suffix:"_from_protobuf_bare" lid) !default_loc) in
      [%expr [%e ident] decoder]
    (* immediate *)
    | Pbt_imm _, Pbe_bytes ->
      let ident = evar ("_" ^ pbf_name ^ "_from_protobuf") in
      [%expr [%e ident] (Protobuf.Decoder.nested decoder)]
    | Pbt_imm _, Pbe_varint ->
      let ident = evar ("_" ^ pbf_name ^ "_from_protobuf_bare") in
      [%expr [%e ident] decoder]
    (* poly *)
    | Pbt_poly var, Pbe_bytes ->
      [%expr [%e evar ("poly_" ^ var)] (Protobuf.Decoder.nested decoder)]
    (* packed *)
    | ty, Pbe_packed pbf_enc ->
      let _reader = mk_reader { field with pbf_enc } in
      [%expr assert false]
    | _ -> assert false
  in
  let rec mk_field_cases fields =
    match fields with
    | { pbf_key; pbf_name; pbf_enc; pbf_type; pbf_kind; pbf_path } as field :: rest ->
      begin match pbf_kind, pbf_enc with
      | Pbk_repeated, ((Pbe_varint | Pbe_zigzag | Pbe_bits64 | Pbe_bits32) as pbf_enc
                      | Pbe_packed pbf_enc) ->
        (* always recognize packed fields *)
        [Exp.case [%pat? (Some ([%p pint pbf_key], Protobuf.Bytes))]
                  [%expr [%e evar pbf_name] :=
                           (let decoder = Protobuf.Decoder.nested decoder in
                            let rec read rest =
                              let value = [%e mk_reader { field with pbf_enc }] in
                              if Protobuf.Decoder.at_end decoder then value :: rest
                              else read (value :: rest)
                            in read ![%e evar pbf_name]); read ()]]
      | _ -> []
      end @
      let pbf_enc, updated =
        match pbf_enc, pbf_kind with
        | pbf_enc, (Pbk_required | Pbk_optional) ->
          pbf_enc, [%expr Some [%e mk_reader field]]
        | (Pbe_packed pbf_enc | pbf_enc), Pbk_repeated ->
          pbf_enc, [%expr [%e mk_reader field] :: ![%e evar pbf_name]]
      in
      let payload_enc = string_payload_kind_of_pb_encoding pbf_enc in
      (Exp.case [%pat? Some ([%p pint pbf_key], [%p pconstr payload_enc []])]
                [%expr [%e evar pbf_name] := [%e updated]; read ()]) ::
      (Exp.case [%pat? Some ([%p pint pbf_key], kind)]
                [%expr raise Protobuf.Decoder.(Failure
                              (Unexpected_payload ([%e str pbf_path], kind)))]) ::
      mk_field_cases rest
    | [] -> []
  in
  let matcher =
    Exp.match_ [%expr Protobuf.Decoder.key decoder]
               ((mk_field_cases fields) @
                [Exp.case [%pat? Some (_, kind)]
                          [%expr Protobuf.Decoder.skip decoder kind; read ()];
                 Exp.case [%pat? None] [%expr ()]])
  in
  let construct_ptyp pbf_name ptyp =
    let { pbf_path; pbf_default } =
      fields |> List.find (fun { pbf_name = pbf_name' } -> pbf_name' = pbf_name)
    in
    match ptyp with
    | [%type: [%t? _] option] ->
      [%expr ![%e evar pbf_name]]
    | [%type: [%t? _] list] ->
      [%expr List.rev (![%e evar pbf_name])]
    | [%type: [%t? _] array] ->
      [%expr Array.of_list (List.rev (![%e evar pbf_name]))]
    | { ptyp_desc = (Ptyp_constr _ | Ptyp_tuple _ | Ptyp_variant _ | Ptyp_var _); } ->
      let default = [%expr raise Protobuf.Decoder.(Failure (Missing_field [%e str pbf_path]))] in
      let default = Option.default default pbf_default in
      [%expr match ![%e evar pbf_name] with None -> [%e default] | Some v -> v ]
    | _ -> assert false
  in
  let mk_variant ptype_name ptype_loc mk_constr constrs =
    let with_args =
      constrs |> ExtList.List.filter_map (fun pcd ->
        match pcd with
        | (name, [],   attrs) -> None
        | (name, args, attrs) -> Some name)
    in
    let rec mk_variant_cases constrs =
      match constrs with
      | (name, args, attrs) :: rest ->
        let field = try  Some (List.find (fun { pbf_name } -> pbf_name = "constr_" ^ name) fields)
                    with Not_found -> None in
        let pkey  = [%pat? Some [%p Pat.constant (Const_int64
                              (Int64.of_int (pb_key_of_attrs attrs)))]] in
        let pargs =
          with_args |> List.map (fun name' ->
            let field' = List.find (fun { pbf_name } -> pbf_name = "constr_" ^ name') fields in
            match field'.pbf_kind with
            | Pbk_required -> if name = name' then [%pat? Some arg] else [%pat? None]
            | Pbk_optional -> if name = name' then [%pat? arg] else [%pat? None]
            | Pbk_repeated -> if name = name' then [%pat? arg] else [%pat? []])
        in
        begin match args with
        | [] ->
          Exp.case (ptuple (pkey :: pargs))
                   (mk_constr name [])
        | [arg] ->
          begin match field, arg with
          | Some { pbf_kind = (Pbk_required | Pbk_optional) }, _ ->
            Exp.case (ptuple (pkey :: pargs))
                     (mk_constr name [[%expr arg]])
          | Some { pbf_kind = Pbk_repeated }, [%type: [%t? _] list] ->
            Exp.case (ptuple (pkey :: pargs))
                     (mk_constr name [[%expr List.rev arg]])
          | Some { pbf_kind = Pbk_repeated }, [%type: [%t? _] array] ->
            Exp.case (ptuple (pkey :: pargs))
                     (mk_constr name [[%expr Array.of_list (List.rev arg)]])
          | _ -> assert false
          end
        | args' -> (* Annoying constructor corner case *)
          let pargs', eargs' =
            List.mapi (fun i _ ->
              let name = Printf.sprintf "a%d" i in pvar name, evar name) args' |>
            List.split
          in
          Exp.case (ptuple (pkey :: pargs))
                   [%expr let [%p ptuple pargs'] = arg in [%e mk_constr name eargs']]
        end :: mk_variant_cases rest
      | [] ->
        let name = (module_name ()) ^ "." ^ ptype_name.txt in
        [Exp.case [%pat? _] [%expr raise Protobuf.Decoder.
                              (Failure (Malformed_variant [%e str name]))]]
    in
    Exp.match_ (tuple ([%expr !variant] ::
                       List.map (fun name -> [%expr ![%e evar ("constr_" ^ name)]]) with_args))
               (mk_variant_cases constrs)
  in
  let constructor =
    match ptype with
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some { ptyp_desc = Ptyp_tuple ptyps } } ->
      Exp.tuple (List.mapi (fun i ptyp ->
        construct_ptyp (Printf.sprintf "elem_%d" i) ptyp) ptyps)
    | { ptype_name; ptype_loc;
        ptype_kind = Ptype_abstract;
        ptype_manifest = Some { ptyp_desc = Ptyp_variant (rows, _, _) } } ->
      rows |> List.map (fun row_field ->
        match row_field with
        | Rtag (name, attrs, _, args) -> (name, args, attrs)
        | _ -> assert false) |> mk_variant ptype_name ptype_loc
          (fun name args ->
            match args with
            | []    -> Exp.variant name None
            | [arg] -> Exp.variant name (Some arg)
            | args  -> Exp.variant name (Some (tuple args)))
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some ptyp } ->
      construct_ptyp "alias" ptyp
    | { ptype_kind = (Ptype_abstract | Ptype_open) } ->
      assert false
    | { ptype_kind = Ptype_record fields; } ->
      Exp.record (List.mapi (fun i { pld_name; pld_type; } ->
        lid pld_name.txt, construct_ptyp ("field_" ^ pld_name.txt) pld_type) fields) None
    | { ptype_kind = Ptype_variant constrs; ptype_name; ptype_loc } ->
      constrs |> List.map (fun { pcd_name = { txt = name}; pcd_args; pcd_attributes; } ->
        name, pcd_args, pcd_attributes) |> mk_variant ptype_name ptype_loc constr
  in
  let read =
    [%expr let rec read () = [%e matcher] in read (); [%e constructor]] |>
    mk_cells fields |>
    mk_imm_readers fields
  in
  Vb.mk (pvar (ptype.ptype_name.txt ^ "_from_protobuf"))
        (mk_poly ptype.ptype_params [%expr fun decoder -> [%e read]])

let derive_writer_bare fields ptype =
  let mk_variant mk_pconstr constrs =
    let rec mk_variant_cases constrs =
      match constrs with
      | (name, attrs) :: rest ->
        (Exp.case (mk_pconstr name)
                  (Exp.constant (Const_int64
                    (Int64.of_int (pb_key_of_attrs attrs))))) ::
        mk_variant_cases rest
      | [] -> []
    in
    let matcher = Exp.match_ [%expr value] (mk_variant_cases constrs) in
    let writer  = [%expr Protobuf.Encoder.varint [%e matcher] encoder] in
    Some (Vb.mk (pvar (ptype.ptype_name.txt ^ "_to_protobuf_bare"))
                [%expr fun value encoder -> [%e writer]])
  in
  match ptype with
  | { ptype_kind = Ptype_variant constrs } when
      List.for_all (fun { pcd_args } -> pcd_args = []) constrs ->
    constrs |> List.map (fun { pcd_name = { txt = name }; pcd_attributes } ->
                  name, pcd_attributes) |> mk_variant (fun name -> pconstr name [])
  | { ptype_kind = Ptype_abstract;
      ptype_manifest = Some { ptyp_desc = Ptyp_variant (rows, _, _) } } when
        List.for_all (fun row_field ->
          match row_field with Rtag (_, _, _, []) -> true | _ -> false) rows ->
    rows |> List.map (fun row_field ->
      match row_field with
      | Rtag (name, attrs, _, []) -> (name, attrs)
      | _ -> assert false) |> mk_variant (fun name -> Pat.variant name None)
  | _ -> None

let rec derive_writer fields ptype =
  let rec mk_imm_writers fields k =
    match fields with
    | { pbf_type = Pbt_imm ty; pbf_name; pbf_path; } :: rest ->
      (* Manufacture a structure just for this immediate *)
      let ptype = Type.mk ~manifest:ty (mkloc ("_" ^ pbf_name) !default_loc) in
      Exp.let_ Nonrecursive
               ((derive_writer (fields_of_ptype pbf_path ptype) ptype) ::
                (Option.map_default (fun x -> [x]) [] (derive_writer_bare fields ptype)))
               (mk_imm_writers rest k)
    | _ :: rest -> mk_imm_writers rest k
    | [] -> k
  in
  let mk_value_writer { pbf_name; pbf_path; pbf_enc; pbf_type; } =
    let encode v =
      let ident = Exp.ident (lid ("Protobuf.Encoder." ^ (string_of_pb_encoding pbf_enc))) in
      [%expr [%e ident] [%e v] encoder]
    in
    match pbf_type, pbf_enc with
    (* bool *)
    | Pbt_bool, Pbe_varint ->
      encode [%expr if [%e evar pbf_name] then 1L else 0L]
    (* int *)
    | Pbt_int, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      encode [%expr Int64.of_int [%e evar pbf_name]]
    | Pbt_int, Pbe_bits32 ->
      encode [%expr Protobuf.Encoder.int32_of_int [%e str pbf_path] [%e evar pbf_name]]
    (* int32 *)
    | Pbt_int32, Pbe_bits32 -> encode (evar pbf_name)
    | Pbt_int32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      encode [%expr Int64.of_int32 [%e evar pbf_name]]
    (* int64 *)
    | Pbt_int64, (Pbe_varint | Pbe_zigzag | Pbe_bits64) -> encode (evar pbf_name)
    | Pbt_int64, Pbe_bits32 ->
      encode [%expr Protobuf.Encoder.int32_of_int64 [%e str pbf_path] [%e evar pbf_name]]
    (* uint32 *)
    | Pbt_uint32, Pbe_bits32 ->
      encode [%expr Uint32.to_int32 [%e evar pbf_name]]
    | Pbt_uint32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      encode [%expr Int64.of_int32 (Uint32.to_int32 [%e evar pbf_name])]
    (* uint64 *)
    | Pbt_uint64, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      encode [%expr Uint64.to_int64 [%e evar pbf_name]]
    | Pbt_uint64, Pbe_bits32 ->
      encode [%expr Protobuf.Encoder.int32_of_int64 [%e str pbf_path]
                      (Uint64.to_int64 [%e evar pbf_name])]
    (* float *)
    | Pbt_float, Pbe_bits32 ->
      encode [%expr Int32.bits_of_float [%e evar pbf_name]]
    | Pbt_float, Pbe_bits64 ->
      encode [%expr Int64.bits_of_float [%e evar pbf_name]]
    (* string *)
    | Pbt_string, Pbe_bytes -> encode (evar pbf_name)
    (* variant *)
    | Pbt_variant _, Pbe_varint -> encode (evar pbf_name)
    (* nested *)
    | Pbt_nested (args, lid), Pbe_bytes ->
      let ident = Exp.ident (mkloc (mangle_lid ~suffix:"_to_protobuf" lid) !default_loc) in
      let args' = args |> List.map (fun ptyp ->
        match ptyp with
        | { ptyp_desc = Ptyp_var tvar } -> evar ("poly_" ^ tvar)
        | _ -> raise (Error (Pberr_wrong_tparm ptyp)))
      in
      [%expr Protobuf.Encoder.nested [%e app ident (args' @ [evar pbf_name])] encoder]
    | Pbt_nested ([], lid), Pbe_varint -> (* bare enum *)
      let ident = Exp.ident (mkloc (mangle_lid ~suffix:"_to_protobuf_bare" lid) !default_loc) in
      [%expr ([%e ident] [%e evar pbf_name]) encoder]
    (* immediate *)
    | Pbt_imm _, Pbe_bytes ->
      let ident = evar ("_" ^ pbf_name ^ "_to_protobuf") in
      [%expr Protobuf.Encoder.nested ([%e ident] [%e evar pbf_name]) encoder]
    | Pbt_imm _, Pbe_varint ->
      let ident = evar ("_" ^ pbf_name ^ "_to_protobuf_bare") in
      [%expr [%e ident] [%e evar pbf_name] encoder]
    (* poly *)
    | Pbt_poly var, Pbe_bytes ->
      [%expr Protobuf.Encoder.nested ([%e evar ("poly_" ^ var)] [%e evar pbf_name]) encoder]
    | _ -> assert false
  in
  let mk_writer ({ pbf_name; pbf_kind; pbf_key; pbf_enc; pbf_default } as field) =
    let key_writer   = [%expr Protobuf.Encoder.key ([%e int pbf_key ],
                          [%e constr (string_payload_kind_of_pb_encoding pbf_enc) []]) encoder] in
    match pbf_kind, pbf_enc with
    | Pbk_required, _ ->
      let writer = [%expr [%e key_writer]; [%e mk_value_writer field]] in
      begin match pbf_default with
      | Some default -> [%expr if [%e evar pbf_name] <> [%e default] then [%e writer]]
      | None -> writer
      end
    | Pbk_optional, _ ->
      [%expr
        match [%e evar pbf_name] with
        | Some [%p pvar pbf_name] -> [%e key_writer]; [%e mk_value_writer field]
        | None -> ()]
    | Pbk_repeated, Pbe_packed pbf_enc ->
      let value_writer = mk_value_writer { field with pbf_enc } in
      [%expr
        if [%e evar pbf_name] <> [] then begin
          [%e key_writer];
          Protobuf.Encoder.nested (fun encoder ->
              List.iter (fun [%p pvar pbf_name] -> [%e value_writer]) [%e evar pbf_name])
            encoder
        end]
    | Pbk_repeated, _ ->
      [%expr
        List.iter (fun [%p pvar pbf_name] ->
          [%e key_writer]; [%e mk_value_writer field]) [%e evar pbf_name]]
  in
  let mk_writers fields =
    List.fold_right (fun field k ->
      [%expr [%e mk_writer field]; [%e k]]) fields [%expr ()]
  in
  let deconstruct_ptyp pbf_name ptyp k =
    match ptyp with
    | [%type: [%t? _] array] ->
      [%expr let [%p pvar pbf_name] = Array.to_list [%e evar pbf_name] in [%e k]]
    | [%type: [%t? _] option]
    | [%type: [%t? _] list]
    | { ptyp_desc = (Ptyp_constr _ | Ptyp_tuple _ | Ptyp_variant _ | Ptyp_var _); } ->
      k
    | _ -> assert false
  in
  let mk_variant mk_patt constrs =
    let rec mk_variant_cases constrs =
      match constrs with
      | (name, [], attrs)  :: rest ->
        let ekey  = Exp.constant (Const_int64 (Int64.of_int (pb_key_of_attrs attrs))) in
        (Exp.case (mk_patt name []) [%expr Protobuf.Encoder.varint [%e ekey] encoder]) ::
        mk_variant_cases rest
      | (name, [arg], attrs) :: rest ->
        let ekey  = Exp.constant (Const_int64 (Int64.of_int (pb_key_of_attrs attrs))) in
        let field = List.find (fun { pbf_name } -> pbf_name = "constr_" ^ name) fields in
        (Exp.case (mk_patt name [pvar ("constr_" ^ name)])
                  (deconstruct_ptyp field.pbf_name arg
                    [%expr
                      Protobuf.Encoder.varint [%e ekey] encoder;
                      [%e mk_writer field]])) ::
        mk_variant_cases rest
      | (name, args, attrs) :: rest ->
        let ekey  = Exp.constant (Const_int64 (Int64.of_int (pb_key_of_attrs attrs))) in
        let field = List.find (fun { pbf_name } -> pbf_name = "constr_" ^ name) fields in
        let argns = List.mapi (fun i _ -> "a" ^ (string_of_int i)) args in
        (Exp.case (mk_patt name (List.map pvar argns))
                  [%expr
                    Protobuf.Encoder.varint [%e ekey] encoder;
                    let [%p pvar ("constr_" ^ name)] = [%e tuple (List.map evar argns)] in
                    [%e mk_writer field]]) ::
        mk_variant_cases rest
      | [] -> []
    in
    [%expr
      Protobuf.Encoder.key (1, Protobuf.Varint) encoder;
      [%e Exp.match_ (evar "value") (mk_variant_cases constrs)]]
  in
  let mk_deconstructor fields =
    match ptype with
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some { ptyp_desc = Ptyp_tuple ptyps } } ->
      [%expr
        let [%p Pat.tuple (List.mapi (fun i _ ->
                  pvar (Printf.sprintf "elem_%d" i)) ptyps)] = value in
        [%e List.fold_right (fun (i, ptyp) k ->
                deconstruct_ptyp (Printf.sprintf "elem_%d" i) ptyp k)
              (List.mapi (fun i ptyp -> i, ptyp) ptyps)
              (mk_writers fields)]]
    | { ptype_kind = Ptype_abstract;
        ptype_manifest = Some { ptyp_desc = Ptyp_variant (rows, _, _) } } ->
      mk_variant (fun name args ->
          Pat.variant name (
            match args with
            | []  -> None
            | [x] -> Some x
            | _   -> Some (ptuple args)))
        (List.map (fun row_field ->
          match row_field with
          | Rtag (name, attrs, _, args) -> (name, args, attrs)
          | _ -> assert false) rows)

    | { ptype_kind = Ptype_abstract; ptype_manifest = Some ptyp } ->
      [%expr let alias = value in
             [%e deconstruct_ptyp "alias" ptyp (mk_writers fields)]]
    | { ptype_kind = (Ptype_abstract | Ptype_open) } ->
      assert false
    | { ptype_kind = Ptype_record ldecls; } ->
      [%expr
        let [%p Pat.record (List.map (fun { pld_name } ->
                  let label = lid pld_name.txt in
                  let patt  = pvar (Printf.sprintf "field_%s" pld_name.txt) in
                  label, patt) ldecls) Closed] = value in
        [%e List.fold_right (fun { pld_name; pld_type } k ->
                deconstruct_ptyp (Printf.sprintf "field_%s" pld_name.txt) pld_type k) ldecls
              (mk_writers fields)]]
    | { ptype_kind = Ptype_variant constrs; ptype_name; ptype_loc } ->
      mk_variant pconstr
        (List.map (fun { pcd_name = { txt = name }; pcd_args; pcd_attributes } ->
          (name, pcd_args, pcd_attributes)) constrs)
  in
  let write = mk_deconstructor fields |> mk_imm_writers fields in
  Vb.mk (pvar (ptype.ptype_name.txt ^ "_to_protobuf"))
        (mk_poly ptype.ptype_params [%expr fun value encoder -> [%e write]])

let derive item =
  match item with
  | { pstr_desc = Pstr_type ty_decls } as item ->
    let derived =
      ty_decls |>
      List.map (fun ({ ptype_name = { txt = name }; ptype_loc } as ptype) ->
        let fields = fields_of_ptype ((module_name ()) ^ "." ^ name) ptype in
        [derive_reader fields ptype] @
        (Option.map_default (fun x -> [x]) [] (derive_reader_bare fields ptype)) @
        [derive_writer fields ptype] @
        (Option.map_default (fun x -> [x]) [] (derive_writer_bare fields ptype))) |>
      List.concat
    in
    [item; Str.value Recursive derived]
  | _ -> assert false

let protobuf_mapper argv =
  { default_mapper with
    structure = fun mapper items ->
      let rec map_types items =
        match items with
        | { pstr_desc = Pstr_type ty_decls } as item :: rest when
            List.exists (fun ty -> has_attr "protobuf" ty.ptype_attributes) ty_decls ->
          derive item @ map_types rest
        | item :: rest ->
          mapper.structure_item mapper item :: map_types rest
        | [] -> []
      in
      map_types items
  }

let () = run_main protobuf_mapper
