#if OCAML_VERSION < (4, 03, 0)
#define Pconst_string Const_string
#endif

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
| Pbt_bytes
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
  pbf_extname : string;
  pbf_path    : string list;
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
| Pberr_dumb_protoc   of Location.t
| Pberr_ocaml_expr    of Location.t

exception Error of error

let filter_map f lst =
  let rec filter result lst =
    match lst with
    | Some x :: lst -> filter (x :: result) lst
    | None   :: lst -> filter result lst
    | [] -> result
  in
  List.map f lst |> filter []

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
  | "varint" -> Some Pbe_varint
  | "zigzag" -> Some Pbe_zigzag
  | "bits32" -> Some Pbe_bits32
  | "bits64" -> Some Pbe_bits64
  | "bytes"  -> Some Pbe_bytes
  | _ -> None

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
  | Pbt_bytes  -> "bytes"
  | Pbt_imm ptyp ->
    Ppx_deriving.string_of_core_type ptyp
  | Pbt_variant constrs ->
    String.concat " | " (List.map snd constrs)
  | Pbt_nested (args, lid) ->
    begin match args with
    | []   -> ""
    | args -> Printf.sprintf "(%s) " (String.concat ", "
                                        (List.map Ppx_deriving.string_of_core_type args))
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
      | `Encoding -> "encoding", "one of: `bool, `varint, `zigzag, `bits32, `bits64, " ^
                                 "`bytes, e.g. [@encoding `varint]"
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
    errorf ~loc "Type %s does not have a Protobuf mapping"
                (Ppx_deriving.string_of_core_type ptyp)
  | Pberr_wrong_tparm ({ ptyp_loc = loc } as ptyp) ->
    errorf ~loc "Type %s cannot be used as a type parameter"
                (Ppx_deriving.string_of_core_type ptyp)
  | Pberr_no_conversion (loc, kind, enc) ->
    errorf ~loc "\"%s\" is not a valid representation for %s"
                      (string_of_pb_encoding enc) (string_of_pb_type kind)
  | Pberr_packed_bytes loc ->
    errorf ~loc "Only fields with varint, bits32 or bits64 encoding may be packed"
  | Pberr_dumb_protoc loc ->
    errorf ~loc "Parametric types are not supported when exporting to protoc"
  | Pberr_ocaml_expr loc ->
    errorf ~loc "Nontrivial OCaml expressions cannot be exported to protoc"

let () =
  Location.register_error_of_exn (fun exn ->
    match exn with
    | Error err -> Some (describe_error err)
    | _ -> None)

let deriver = "protobuf"

let pb_key_of_attrs attrs =
  match Ppx_deriving.attr ~deriver "key" attrs with
#if OCAML_VERSION < (4, 03, 0)
  | Some ({ loc }, PStr [[%stri [%e? { pexp_desc = Pexp_constant (Const_int key) }]]]) ->
#else
  | Some ({ loc }, PStr [[%stri [%e? { pexp_desc = Pexp_constant (Pconst_integer (sn, _)) }]]]) ->
    let key = int_of_string sn in 
#endif
    if key < 1 || key > 0x1fffffff || (key >= 19000 && key <= 19999) then
      raise (Error (Pberr_key_invalid (loc, key)));
    Some key
  | Some ({ loc }, _) -> raise (Error (Pberr_attr_syntax (loc, `Key)))
  | None -> None

let pb_encoding_of_attrs attrs =
  match Ppx_deriving.attr ~deriver "encoding" attrs with
  | Some ({ loc }, PStr [[%stri [%e? { pexp_desc = Pexp_variant (kind, None) }]]]) ->
    begin match pb_encoding_of_string kind with
    | Some x -> Some x
    | None -> raise (Error (Pberr_attr_syntax (loc, `Encoding)))
    end
  | Some ({ loc }, _) -> raise (Error (Pberr_attr_syntax (loc, `Encoding)))
  | None -> None

let bare_of_attrs attrs =
  match Ppx_deriving.attr ~deriver "bare" attrs with
  | Some (_, PStr []) -> true
  | Some ({ loc }, _) -> raise (Error (Pberr_attr_syntax (loc, `Bare)))
  | None -> false

let default_of_attrs attrs =
  match Ppx_deriving.attr ~deriver "default" attrs with
  | Some (_, PStr [[%stri [%e? expr]]]) -> Some expr
  | Some ({ loc }, _) -> raise (Error (Pberr_attr_syntax (loc, `Default)))
  | None -> None

let packed_of_attrs attrs =
  match Ppx_deriving.attr ~deriver "packed" attrs with
  | Some (_, PStr []) -> true
  | Some ({ loc }, _) -> raise (Error (Pberr_attr_syntax (loc, `Packed)))
  | None -> false

let fields_of_ptype base_path ptype =
  let rec field_of_ptyp pbf_name pbf_extname pbf_path pbf_key pbf_kind ptyp =
    match ptyp with
    | [%type: [%t? arg] option] ->
      begin match pbf_kind with
      | Pbk_required ->
        field_of_ptyp pbf_name pbf_extname pbf_path pbf_key Pbk_optional
          { arg with ptyp_attributes = ptyp.ptyp_attributes @ arg.ptyp_attributes }
      | _ -> raise (Error (Pberr_wrong_ty ptyp))
      end
    | [%type: [%t? arg] array] | [%type: [%t? arg] list] ->
      begin match pbf_kind with
      | Pbk_required ->
        let { pbf_enc } as field =
          field_of_ptyp pbf_name pbf_extname pbf_path pbf_key Pbk_repeated
            { arg with ptyp_attributes = ptyp.ptyp_attributes @ arg.ptyp_attributes }
        in
        let pbf_enc =
          if packed_of_attrs ptyp.ptyp_attributes then Pbe_packed pbf_enc else pbf_enc
        in
        if pbf_enc = Pbe_packed Pbe_bytes then
          raise (Error (Pberr_packed_bytes ptyp.ptyp_loc));
        { field with pbf_enc }
      | _ -> raise (Error (Pberr_wrong_ty ptyp))
      end
    | { ptyp_desc = (Ptyp_tuple _ | Ptyp_variant _ | Ptyp_var _) as desc;
        ptyp_attributes = attrs; ptyp_loc; } ->
      let pbf_key =
        match pb_key_of_attrs attrs with
        | Some key -> key
        | None ->
          match pbf_key with
          | Some k -> k
          | None -> raise (Error (Pberr_no_key ptyp_loc))
      in
      let pbf_enc, pbf_type =
        match desc with
        | Ptyp_variant _ ->
          (if bare_of_attrs attrs then Pbe_varint else Pbe_bytes), Pbt_imm ptyp
        | Ptyp_tuple _   -> Pbe_bytes, Pbt_imm ptyp
        | Ptyp_var var   -> Pbe_bytes, Pbt_poly var
        | _ -> assert false
      in
      { pbf_name; pbf_extname; pbf_key; pbf_kind; pbf_path; pbf_type; pbf_enc;
        pbf_loc     = ptyp_loc;
        pbf_default = default_of_attrs attrs; }
    | { ptyp_desc = Ptyp_constr ({ txt = lid }, args); ptyp_attributes = attrs; ptyp_loc; } ->
      let pbf_type =
        match args, lid with
        | [], Lident "bool"   -> Pbt_bool
        | [], Lident "int"    -> Pbt_int
        | [], Lident "float"  -> Pbt_float
        | [], (Lident "string" | Ldot (Lident "String", "t")) -> Pbt_string
        | [], (Lident "bytes"  | Ldot (Lident "Bytes", "t"))  -> Pbt_bytes
        | [], (Lident "int32"  | Ldot (Lident "Int32", "t"))  -> Pbt_int32
        | [], (Lident "int64"  | Ldot (Lident "Int64", "t"))  -> Pbt_int64
        | [], (Lident "uint32" | Ldot (Lident "Uint32", "t")) -> Pbt_uint32
        | [], (Lident "uint64" | Ldot (Lident "Uint64", "t")) -> Pbt_uint64
        | _,  lident -> Pbt_nested (args, lident)
      in
      let pbf_key =
        match pb_key_of_attrs attrs with
        | Some key -> key
        | None ->
          match pbf_key with
          | Some k -> k
          | None -> raise (Error (Pberr_no_key ptyp_loc))
      in
      let pbf_enc =
        match pb_encoding_of_attrs attrs with
        | Some enc -> enc
        | None ->
          match pbf_type with
          | Pbt_float  -> Pbe_bits64
          | Pbt_bool   | Pbt_int     -> Pbe_varint
          | Pbt_int32  | Pbt_uint32  -> Pbe_bits32
          | Pbt_int64  | Pbt_uint64  -> Pbe_bits64
          | Pbt_string | Pbt_bytes
          | Pbt_imm _  | Pbt_poly _  -> Pbe_bytes
          | Pbt_nested _ ->
            if bare_of_attrs attrs then Pbe_varint else Pbe_bytes
          | Pbt_variant _ -> assert false
      in
      begin match pbf_type, pbf_enc with
      | Pbt_bool, Pbe_varint
      | (Pbt_int | Pbt_int32 | Pbt_int64 | Pbt_uint32 | Pbt_uint64),
        (Pbe_varint | Pbe_zigzag | Pbe_bits32 | Pbe_bits64)
      | Pbt_float, (Pbe_bits32 | Pbe_bits64)
      | (Pbt_string | Pbt_bytes), Pbe_bytes
      | Pbt_nested _, (Pbe_bytes | Pbe_varint) ->
        { pbf_name; pbf_extname; pbf_key; pbf_enc; pbf_type; pbf_kind; pbf_path;
          pbf_loc     = ptyp_loc;
          pbf_default = default_of_attrs attrs }
      | _ ->
        raise (Error (Pberr_no_conversion (ptyp_loc, pbf_type, pbf_enc)))
      end
    | { ptyp_desc = Ptyp_alias (ptyp', _) } ->
      field_of_ptyp pbf_name pbf_extname pbf_path pbf_key pbf_kind ptyp'
    | ptyp -> raise (Error (Pberr_wrong_ty ptyp))
  in
  let fields_of_variant loc constrs =
    let constrs' =
      constrs |> List.map (fun ((name, args, attrs, loc) as pcd) ->
        match pb_key_of_attrs attrs with
        | Some key -> key, pcd
        | None -> raise (Error (Pberr_no_key loc)))
    in
    constrs' |> List.iter (fun (key, (_, _, _, loc) as pcd) ->
      constrs' |> List.iter (fun (key', (_, _, _, loc') as pcd') ->
        if pcd != pcd' && key = key' then
          raise (Error (Pberr_key_duplicate (key, loc', loc)))));
    { pbf_name    = "variant";
      pbf_extname = "tag";
      pbf_path    = base_path;
      pbf_key     = 1;
      pbf_enc     = Pbe_varint;
      pbf_type    = Pbt_variant (constrs' |> List.map (fun (key, (name, _, _, _)) -> key, name));
      pbf_kind    = Pbk_required;
      pbf_loc     = loc;
      pbf_default = None; } ::
    (constrs |> filter_map (fun (name, args, attrs, loc) ->
      let ptyp =
        match args with
        | []    -> None
        | [arg] -> Some arg
        | args  -> Some (Typ.tuple args)
      in
      match ptyp with
      | Some ptyp ->
        let key = 1 + (match pb_key_of_attrs attrs with
                       | Some key -> key | None -> assert false) in
        Some (field_of_ptyp (Printf.sprintf "constr_%s" name) name
                            (base_path @ [name]) (Some key) Pbk_required ptyp)
      | None -> None))
  in
  let fields =
    match ptype with
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some { ptyp_desc = Ptyp_tuple ptyps } } ->
      ptyps |> List.mapi (fun i ptyp ->
        field_of_ptyp (Printf.sprintf "elem_%d" i) (Printf.sprintf "_%d" i)
                      (base_path @ [string_of_int i]) (Some (i + 1)) Pbk_required ptyp)
    | { ptype_kind = Ptype_abstract;
        ptype_manifest = Some ({ ptyp_desc = Ptyp_variant (rows, _, _); ptyp_loc } as ptyp);
        ptype_loc; } ->
      rows |> List.map (fun row_field ->
        match row_field with
        | Rtag (name, attrs, _, [])  -> (name, [], attrs, ptyp_loc)
        | Rtag (name, attrs, _, [a]) -> (name, [a], attrs, ptyp_loc)
        | _ -> raise (Error (Pberr_wrong_ty ptyp))) |>
      fields_of_variant ptype_loc
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some ptyp } ->
      [field_of_ptyp "alias" "_" base_path (Some 1) Pbk_required ptyp]
    | { ptype_kind = Ptype_abstract; ptype_manifest = None } ->
      raise (Error (Pberr_abstract ptype))
    | { ptype_kind = Ptype_open } ->
      raise (Error (Pberr_open ptype))
    | { ptype_kind = Ptype_record fields } ->
      fields |> List.mapi (fun i { pld_name = { txt = name }; pld_type; pld_attributes; } ->
        field_of_ptyp ("field_" ^ name) name (base_path @ [name]) None Pbk_required
                      { pld_type with ptyp_attributes = pld_attributes @ pld_type.ptyp_attributes })
    | { ptype_kind = Ptype_variant constrs; ptype_loc } ->
      constrs 
      |> List.map (fun { pcd_name = { txt = name }; pcd_args; pcd_attributes; pcd_loc; } ->
       (name, pcd_args, pcd_attributes, pcd_loc)
      ) 
#if OCAML_VERSION >= (4, 03, 0)
      |> List.map (fun (name, pcd_args, pcd_attributes, pcd_loc) -> 
        match pcd_args with
        | Pcstr_tuple pcd_args -> (name, pcd_args, pcd_attributes, pcd_loc) 
        | Pcstr_record pcd_label_args -> 
          (* For now inline record are treated just like tuple (hence the key will be 
             automatically generated starting at 1)
             
             Since inline records support attributes, protobuf keys could be
             customized:
             
             `| F {f10 [@key 10] : int; f11 [@key 11] :string}` 
          *)
          let pcd_args = List.map (fun {pld_type; _ } -> pld_type) pcd_label_args in 
          (name, pcd_args, pcd_attributes, pcd_loc)
      ) 
#endif
      |> fields_of_variant ptype_loc
  in
  fields |> List.iter (fun field ->
    fields |> List.iter (fun field' ->
      if field != field' && field.pbf_key = field'.pbf_key then
        raise (Error (Pberr_key_duplicate (field.pbf_key, field'.pbf_loc, field.pbf_loc)))));
  fields |> List.sort (fun { pbf_key = a } { pbf_key = b } -> compare a b)

let empty_constructor_argument {pcd_args; _ } = 
#if OCAML_VERSION < (4, 03, 0)
  match pcd_args with
  | [] -> true
  | _ -> false 
#else 
  match pcd_args with
  | Pcstr_tuple   [] | Pcstr_record [] -> true
  | _ -> false 
#endif

let int64_constant_of_int i = 
#if OCAML_VERSION < (4, 03, 0)
  Const_int64 (Int64.of_int i)
#else
  Pconst_integer (string_of_int i, Some 'L') 
#endif

let derive_reader_bare base_path fields ptype =
  let mk_variant mk_constr constrs =
    let rec mk_variant_cases constrs =
      match constrs with
      | (name, attrs) :: rest ->
        let key = match pb_key_of_attrs attrs with Some key -> key | None -> assert false in
        (Exp.case (Pat.constant (int64_constant_of_int key))
                  (mk_constr name)) :: mk_variant_cases rest
      | [] ->
        let field_name = String.concat "." base_path in
        [Exp.case [%pat? _] [%expr raise Protobuf.Decoder.
                              (Failure (Malformed_variant [%e str field_name]))]]
    in
    let matcher =
      Exp.match_ [%expr Protobuf.Decoder.varint decoder]
                 (mk_variant_cases constrs) in
    Some (Vb.mk (pvar (Ppx_deriving.mangle_type_decl (`Suffix "from_protobuf_bare") ptype))
                [%expr fun decoder -> [%e Ppx_deriving.sanitize matcher]])
  in

  match ptype with
  | { ptype_kind = Ptype_variant constrs } when
      List.for_all empty_constructor_argument constrs ->
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

let rec derive_reader base_path fields ptype =
  let rec mk_imm_readers fields k =
    match fields with
    | { pbf_type = Pbt_imm ptyp; pbf_name; pbf_path; } :: rest ->
      (* Manufacture a structure just for this immediate *)
      let ptype  = Type.mk ~manifest:ptyp (mkloc ("_" ^ pbf_name) !default_loc) in
      (* Order is important, derive_reader does less checks than derive_reader_bare. *)
      let reader = derive_reader pbf_path (fields_of_ptype pbf_path ptype) ptype in
      Exp.let_ Nonrecursive
               (reader :: (match derive_reader_bare pbf_path fields ptype with
                           | Some x -> [x] | None -> []))
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
    let field_name = String.concat "." pbf_path in
    match pbf_type, pbf_enc with
    (* bool *)
    | Pbt_bool, Pbe_varint ->
      [%expr Protobuf.Decoder.bool_of_int64 [%e str field_name] [%e value]]
    (* int *)
    | Pbt_int, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Protobuf.Decoder.int_of_int64 [%e str field_name] [%e value]]
    | Pbt_int, Pbe_bits32 ->
      [%expr Protobuf.Decoder.int_of_int32 [%e str field_name] [%e value]]
    (* int32 *)
    | Pbt_int32, Pbe_bits32 -> value
    | Pbt_int32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Protobuf.Decoder.int32_of_int64 [%e str field_name] [%e value]]
    (* int64 *)
    | Pbt_int64, (Pbe_varint | Pbe_zigzag | Pbe_bits64) -> value
    | Pbt_int64, Pbe_bits32 ->
      [%expr Int64.of_int32 [%e value]]
    (* uint32 *)
    | Pbt_uint32, Pbe_bits32 ->
      [%expr Uint32.of_int32 [%e value]]
    | Pbt_uint32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Uint32.of_int32 (Protobuf.Decoder.int32_of_int64 [%e str field_name] [%e value])]
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
    | Pbt_string, Pbe_bytes -> [%expr Bytes.to_string [%e value]]
    (* bytes *)
    | Pbt_bytes, Pbe_bytes -> value
    (* variant *)
    | Pbt_variant _, Pbe_varint -> value
    (* nested *)
    | Pbt_nested (args, lid), Pbe_bytes ->
      let reader lid = Exp.ident (mknoloc (Ppx_deriving.mangle_lid (`Suffix "from_protobuf") lid)) in
      let rec expr_of_core_type ptyp =
        match ptyp with
        | { ptyp_desc = Ptyp_var tvar } -> evar ("poly_" ^ tvar)
        | { ptyp_desc = Ptyp_constr({ txt = lid }, []) } -> reader lid
        | { ptyp_desc = Ptyp_constr({ txt = lid }, ptyps) } ->
          app (reader lid) (List.map expr_of_core_type ptyps)
        | ptyp -> raise (Error (Pberr_wrong_tparm ptyp))
      in
      app (reader lid) ((List.map expr_of_core_type args) @ [[%expr Protobuf.Decoder.nested decoder]])
    | Pbt_nested ([], lid), Pbe_varint -> (* bare enum *)
      let ident = Exp.ident (mknoloc (Ppx_deriving.mangle_lid (`Suffix "from_protobuf_bare") lid)) in
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
      let field_name = String.concat "." pbf_path in
      let payload_enc = string_payload_kind_of_pb_encoding pbf_enc in
      (Exp.case [%pat? Some ([%p pint pbf_key], [%p pconstr payload_enc []])]
                [%expr [%e evar pbf_name] := [%e updated]; read ()]) ::
      (Exp.case [%pat? Some ([%p pint pbf_key], kind)]
                [%expr raise Protobuf.Decoder.(Failure
                              (Unexpected_payload ([%e str field_name], kind)))]) ::
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
      let field_name = String.concat "." pbf_path in
      let default = [%expr raise Protobuf.Decoder.(Failure (Missing_field [%e str field_name]))] in
      let default = match pbf_default with Some x -> x | None  -> default in
      [%expr match ![%e evar pbf_name] with None -> [%e default] | Some v -> v ]
    | _ -> assert false
  in
  let mk_variant ptype_name ptype_loc mk_constr constrs =
    let with_args =
      constrs |> filter_map (fun pcd ->
        match pcd with
        | (name, [],   attrs) -> None
        | (name, args, attrs) -> Some name)
    in
    let rec mk_variant_cases constrs =
      match constrs with
      | (name, args, attrs) :: rest ->
        let field = try  Some (List.find (fun { pbf_name } -> pbf_name = "constr_" ^ name) fields)
                    with Not_found -> None in
        let key = match pb_key_of_attrs attrs with Some key -> key | None -> assert false in
        let pkey  = [%pat? Some [%p Pat.constant (int64_constant_of_int key)]] in
        let pargs =
          with_args |> List.map (fun name' ->
            let field' = List.find (fun { pbf_name } -> pbf_name = "constr_" ^ name') fields in
            match field'.pbf_kind with
            | Pbk_required -> if name = name' then [%pat? Some arg] else [%pat? None]
            | Pbk_optional -> if name = name' then [%pat? arg] else [%pat? None]
            | Pbk_repeated -> if name = name' then [%pat? arg] else [%pat? []])
        in
        let pat = match pargs with [] -> pkey | pargs -> ptuple (pkey :: pargs) in
        begin match args with
        | [] ->
          Exp.case pat (mk_constr name [])
        | [arg] ->
          begin match field, arg with
          | Some { pbf_kind = (Pbk_required | Pbk_optional) }, _ ->
            Exp.case pat (mk_constr name [[%expr arg]])
          | Some { pbf_kind = Pbk_repeated }, [%type: [%t? _] list] ->
            Exp.case pat (mk_constr name [[%expr List.rev arg]])
          | Some { pbf_kind = Pbk_repeated }, [%type: [%t? _] array] ->
            Exp.case pat (mk_constr name [[%expr Array.of_list (List.rev arg)]])
          | _ -> assert false
          end
        | args' -> (* Annoying constructor corner case *)
          let pargs', eargs' =
            List.mapi (fun i _ ->
              let name = Printf.sprintf "a%d" i in pvar name, evar name) args' |>
            List.split
          in
          Exp.case pat [%expr let [%p ptuple pargs'] = arg in [%e mk_constr name eargs']]
        end :: mk_variant_cases rest
      | [] ->
        let field_name = String.concat "." base_path in
        [Exp.case [%pat? _] [%expr raise Protobuf.Decoder.
                              (Failure (Malformed_variant [%e str field_name]))]]
    in
    let input =
      match with_args with
      | []   -> [%expr !variant]
      | args -> (tuple ([%expr !variant] ::
                  List.map (fun name -> [%expr ![%e evar ("constr_" ^ name)]]) with_args))
    in
    Exp.match_ input (mk_variant_cases constrs)
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
      constrs 
      |> List.map (fun { pcd_name = { txt = name}; pcd_args; pcd_attributes; } ->
        name, pcd_args, pcd_attributes
      ) 
#if OCAML_VERSION >= (4, 03, 0)
      |> List.map (fun (name, pcd_args, pcd_attributes) -> 
        match pcd_args with
        | Pcstr_tuple pcd_args -> (name, pcd_args, pcd_attributes) 
        | Pcstr_record pcd_label_args -> 
          let pcd_args = List.map (fun {pld_type; _ } -> pld_type) pcd_label_args in 
          (name, pcd_args, pcd_attributes)
       ) 
#endif 
      |> mk_variant ptype_name ptype_loc constr
  in
  let read =
    [%expr let rec read () = [%e matcher] in read (); [%e constructor]] |>
    mk_cells fields |>
    mk_imm_readers fields
  in
  Vb.mk (pvar (Ppx_deriving.mangle_type_decl (`Suffix "from_protobuf") ptype))
        (Ppx_deriving.poly_fun_of_type_decl ptype
          [%expr fun decoder -> [%e Ppx_deriving.sanitize read]])

let derive_writer_bare fields ptype =
  let mk_variant mk_pconstr constrs =
    let rec mk_variant_cases constrs =
      match constrs with
      | (name, attrs) :: rest ->
        let key = match pb_key_of_attrs attrs with Some key -> key | None -> assert false in
        (Exp.case (mk_pconstr name)
                  (Exp.constant (int64_constant_of_int key))) ::
        mk_variant_cases rest
      | [] -> []
    in
    let matcher = Exp.match_ [%expr value] (mk_variant_cases constrs) in
    let writer  = [%expr Protobuf.Encoder.varint [%e matcher] encoder] in
    Some (Vb.mk (pvar (Ppx_deriving.mangle_type_decl (`Suffix "to_protobuf_bare") ptype))
                [%expr fun value encoder -> [%e Ppx_deriving.sanitize writer]])
  in
  match ptype with
  | { ptype_kind = Ptype_variant constrs } when
      List.for_all empty_constructor_argument constrs ->
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
    | { pbf_type = Pbt_imm ptyp; pbf_name; pbf_path; } :: rest ->
      (* Manufacture a structure just for this immediate *)
      let ptype = Type.mk ~manifest:ptyp (mknoloc ("_" ^ pbf_name)) in
      Exp.let_ Nonrecursive
               ((derive_writer (fields_of_ptype pbf_path ptype) ptype) ::
                (match derive_writer_bare fields ptype with
                 | Some x -> [x] | None -> []))
               (mk_imm_writers rest k)
    | _ :: rest -> mk_imm_writers rest k
    | [] -> k
  in
  let mk_value_writer { pbf_name; pbf_path; pbf_enc; pbf_type; } =
    let encode v =
      let ident = Exp.ident (lid ("Protobuf.Encoder." ^ (string_of_pb_encoding pbf_enc))) in
      [%expr [%e ident] [%e v] encoder]
    in
    let field_name = String.concat "." pbf_path in
    match pbf_type, pbf_enc with
    (* bool *)
    | Pbt_bool, Pbe_varint ->
      encode [%expr if [%e evar pbf_name] then 1L else 0L]
    (* int *)
    | Pbt_int, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      encode [%expr Int64.of_int [%e evar pbf_name]]
    | Pbt_int, Pbe_bits32 ->
      encode [%expr Protobuf.Encoder.int32_of_int [%e str field_name] [%e evar pbf_name]]
    (* int32 *)
    | Pbt_int32, Pbe_bits32 -> encode (evar pbf_name)
    | Pbt_int32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      encode [%expr Int64.of_int32 [%e evar pbf_name]]
    (* int64 *)
    | Pbt_int64, (Pbe_varint | Pbe_zigzag | Pbe_bits64) -> encode (evar pbf_name)
    | Pbt_int64, Pbe_bits32 ->
      encode [%expr Protobuf.Encoder.int32_of_int64 [%e str field_name] [%e evar pbf_name]]
    (* uint32 *)
    | Pbt_uint32, Pbe_bits32 ->
      encode [%expr Uint32.to_int32 [%e evar pbf_name]]
    | Pbt_uint32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      encode [%expr Int64.of_int32 (Uint32.to_int32 [%e evar pbf_name])]
    (* uint64 *)
    | Pbt_uint64, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      encode [%expr Uint64.to_int64 [%e evar pbf_name]]
    | Pbt_uint64, Pbe_bits32 ->
      encode [%expr Protobuf.Encoder.int32_of_int64 [%e str field_name]
                      (Uint64.to_int64 [%e evar pbf_name])]
    (* float *)
    | Pbt_float, Pbe_bits32 ->
      encode [%expr Int32.bits_of_float [%e evar pbf_name]]
    | Pbt_float, Pbe_bits64 ->
      encode [%expr Int64.bits_of_float [%e evar pbf_name]]
    (* string *)
    | Pbt_string, Pbe_bytes -> encode [%expr Bytes.of_string [%e evar pbf_name]]
    (* bytes *)
    | Pbt_bytes, Pbe_bytes -> encode (evar pbf_name)
    (* variant *)
    | Pbt_variant _, Pbe_varint -> encode (evar pbf_name)
    (* nested *)
    | Pbt_nested (args, lid), Pbe_bytes ->
      let writer lid = Exp.ident (mknoloc (Ppx_deriving.mangle_lid (`Suffix "to_protobuf") lid)) in
      let rec expr_of_core_type ptyp =
        match ptyp with
        | { ptyp_desc = Ptyp_var tvar } -> evar ("poly_" ^ tvar)
        | { ptyp_desc = Ptyp_constr({ txt = lid }, []) } -> writer lid
        | { ptyp_desc = Ptyp_constr({ txt = lid }, ptyps) } ->
          app (writer lid) (List.map expr_of_core_type ptyps)
        | _ -> raise (Error (Pberr_wrong_tparm ptyp))
      in
      [%expr Protobuf.Encoder.nested
        [%e app (writer lid) ((List.map expr_of_core_type args) @ [evar pbf_name])] encoder]
    | Pbt_nested ([], lid), Pbe_varint -> (* bare enum *)
      let ident = Exp.ident (mknoloc (Ppx_deriving.mangle_lid (`Suffix "to_protobuf_bare") lid)) in
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
        let key   = match pb_key_of_attrs attrs with Some key -> key | None -> assert false in
        let ekey  = Exp.constant (int64_constant_of_int key) in
        (Exp.case (mk_patt name []) [%expr Protobuf.Encoder.varint [%e ekey] encoder]) ::
        mk_variant_cases rest
      | (name, [arg], attrs) :: rest ->
        let key   = match pb_key_of_attrs attrs with Some key -> key | None -> assert false in
        let ekey  = Exp.constant (int64_constant_of_int key) in
        let field = List.find (fun { pbf_name } -> pbf_name = "constr_" ^ name) fields in
        (Exp.case (mk_patt name [pvar ("constr_" ^ name)])
                  (deconstruct_ptyp field.pbf_name arg
                    [%expr
                      Protobuf.Encoder.varint [%e ekey] encoder;
                      [%e mk_writer field]])) ::
        mk_variant_cases rest
      | (name, args, attrs) :: rest ->
        let key   = match pb_key_of_attrs attrs with Some key -> key | None -> assert false in
        let ekey  = Exp.constant (int64_constant_of_int key) in
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
      constrs
      |> List.map (fun { pcd_name = { txt = name }; pcd_args; pcd_attributes } ->
          (name, pcd_args, pcd_attributes)
      ) 
#if OCAML_VERSION >= (4, 03, 0)
      |> List.map (fun (name, pcd_args, pcd_attributes) -> 
        match pcd_args with
        | Pcstr_tuple pcd_args -> (name, pcd_args, pcd_attributes) 
        | Pcstr_record pcd_label_args -> 
          let pcd_args = List.map (fun {pld_type; _ } -> pld_type) pcd_label_args in 
          (name, pcd_args, pcd_attributes)
      ) 
#endif
      |> mk_variant pconstr

  in
  let write = mk_deconstructor fields |> mk_imm_writers fields in
  Vb.mk (pvar (Ppx_deriving.mangle_type_decl (`Suffix "to_protobuf") ptype))
        (Ppx_deriving.poly_fun_of_type_decl ptype
          [%expr fun value encoder -> [%e Ppx_deriving.sanitize write]])

let str_of_type ~options ~path ({ ptype_name = { txt = name }; ptype_loc } as ptype) =
  let path   = path @ [name] in
  let fields = fields_of_ptype path ptype in
  (* Order is important, writer does less checks than reader. *)
  let reader =
    derive_reader path fields ptype ::
    (match derive_reader_bare path fields ptype with | Some x -> [x] | None -> [])
  in
  let writer =
    derive_writer fields ptype ::
    (match derive_writer_bare fields ptype with | Some x -> [x] | None -> [])
  in
  reader @ writer

let has_bare ptype =
  match ptype with
  | { ptype_kind = Ptype_variant constrs } when
      List.for_all empty_constructor_argument constrs -> true
  | { ptype_kind = Ptype_abstract;
      ptype_manifest = Some { ptyp_desc = Ptyp_variant (rows, _, _) } } when
        List.for_all (fun row_field ->
          match row_field with Rtag (_, _, _, []) -> true | _ -> false) rows -> true
  | _ -> false

let sig_of_type ~options ~path ({ ptype_name = { txt = name } } as ptype) =
  let typ = Ppx_deriving.core_type_of_type_decl ptype in
  let reader_typ = Ppx_deriving.poly_arrow_of_type_decl
    (fun var -> [%type: Protobuf.Decoder.t -> [%t var]]) ptype
    [%type: Protobuf.Decoder.t -> [%t typ]]
  and writer_typ = Ppx_deriving.poly_arrow_of_type_decl
    (fun var -> [%type: [%t var] -> Protobuf.Encoder.t -> unit]) ptype
    [%type: [%t typ] -> Protobuf.Encoder.t -> unit]
  in
  (if not (has_bare ptype) then [] else
    [Sig.value (Val.mk (mknoloc
        (Ppx_deriving.mangle_type_decl (`Suffix "from_protobuf_bare") ptype)) reader_typ);
     Sig.value (Val.mk (mknoloc
        (Ppx_deriving.mangle_type_decl (`Suffix "to_protobuf_bare") ptype)) writer_typ)]) @
  [Sig.value (Val.mk (mknoloc
      (Ppx_deriving.mangle_type_decl (`Suffix "from_protobuf") ptype)) reader_typ);
   Sig.value (Val.mk (mknoloc
      (Ppx_deriving.mangle_type_decl (`Suffix "to_protobuf") ptype)) writer_typ)]

module LongidentSet = Set.Make(struct
  type t = Longident.t
  let compare = compare
end)

let rec write_protoc ~fmt ~path:base_path ?(import=[])
                     ({ ptype_name = { txt = name; loc } } as ptype) =
  let path   = base_path @ [name] in
  let fields = fields_of_ptype path ptype in
  Format.fprintf fmt "@,// %s:%d" loc.loc_start.Lexing.pos_fname loc.loc_end.Lexing.pos_lnum;
  (* import all external definitions *)
  if (List.length import) = 0 then
    let depends = ref LongidentSet.empty in
    fields |> List.iter (fun field ->
      match field.pbf_type with
      | Pbt_nested ([], Ldot (lid, _)) ->
        begin try
          ignore (LongidentSet.find lid !depends)
        with Not_found ->
          depends := LongidentSet.add lid !depends;
          Format.fprintf fmt "@,import \"%s.protoc\";" (String.concat "." (Longident.flatten lid))
        end
      | _ -> ())
  else
    import |> List.iter (Format.fprintf fmt "@,import \"%s\";");
  (* emit a bare variant form *)
  Format.fprintf fmt "@,@[<v 2>message %s {" name;
  fields |> List.iter (fun field ->
    let subname = "_" ^ field.pbf_extname in
    match field.pbf_type with
    | Pbt_variant variants ->
      Format.fprintf fmt "@,@[<v 2>enum %s {" subname;
      variants |> List.iter (fun (key, name) ->
        Format.fprintf fmt "@,%s_tag = %d;" name key);
      Format.fprintf fmt "@]@,}@,";
    | Pbt_imm ptyp ->
      (* Manufacture a structure just for this immediate *)
      let ptype = Type.mk ~manifest:ptyp (mkloc subname !default_loc) in
      write_protoc ~fmt ~path:(path @ [field.pbf_name]) ptype
    | _ -> ());
  let write_field ?(omit_recurrence=false) ({ pbf_extname } as field) =
    let is_packed, pbf_enc =
      match field.pbf_enc with
      | Pbe_packed pbf_enc -> true, pbf_enc
      | pbf_enc -> false, pbf_enc
    in
    let protoc_recurrence =
      match field.pbf_kind with
      | Pbk_required -> "required"
      | Pbk_optional -> "optional"
      | Pbk_repeated -> "repeated"
    and protoc_type =
      match field.pbf_type, pbf_enc with
      | Pbt_bool,      Pbe_varint -> "bool"
      | Pbt_int,       Pbe_varint -> "int64"    (* conservative choice of size *)
      | Pbt_int,       Pbe_zigzag -> "sint64"   (* ditto *)
      | Pbt_int32,     Pbe_varint -> "int32"
      | Pbt_int32,     Pbe_zigzag -> "sint32"
      | Pbt_int64,     Pbe_varint -> "int64"
      | Pbt_int64,     Pbe_zigzag -> "sint64"
      | Pbt_uint32,    Pbe_varint -> "uint32"
      | Pbt_uint32,    Pbe_zigzag -> "sint32"
      | Pbt_uint64,    Pbe_varint -> "uint64"
      | Pbt_uint64,    Pbe_zigzag -> "sint64"
      | (Pbt_int | Pbt_int32 | Pbt_int64 | Pbt_uint32 | Pbt_uint64),
                       Pbe_bits32 -> "sfixed32"
      | (Pbt_int | Pbt_int32 | Pbt_int64 | Pbt_uint32 | Pbt_uint64),
                       Pbe_bits64 -> "sfixed64"
      | Pbt_float,     Pbe_bits32 -> "float"
      | Pbt_float,     Pbe_bits64 -> "double"
      | Pbt_string,    Pbe_bytes  -> "string"
      | Pbt_bytes,     Pbe_bytes  -> "bytes"
      | Pbt_imm _,     Pbe_varint -> "_" ^ pbf_extname ^ "._tag"
      | Pbt_imm _,     Pbe_bytes  -> "_" ^ pbf_extname
      | Pbt_variant _, Pbe_varint -> "_" ^ pbf_extname
      | Pbt_nested ([], lid), Pbe_varint ->
        (String.concat "." (Longident.flatten lid)) ^ "._tag"
      | Pbt_nested ([], lid), Pbe_bytes ->
        String.concat "." (Longident.flatten lid)
      | Pbt_nested(_, _), _
      | Pbt_poly(_), _ ->
        raise (Error (Pberr_dumb_protoc field.pbf_loc))
      | _ -> assert false
    in
    Format.fprintf fmt "@,";
    if omit_recurrence then assert (field.pbf_kind == Pbk_required)
                       else Format.fprintf fmt "%s " protoc_recurrence;
    Format.fprintf fmt "%s %s = %d" protoc_type pbf_extname field.pbf_key;
    if is_packed then
      Format.fprintf fmt " [packed=true]";
    let escape ~pass_8bit s =
      let buf = Buffer.create (String.length s) in
      s |> String.iter (fun c ->
        match c with
        | '\x00'..'\x19' ->
          Buffer.add_string buf (Printf.sprintf "\\x%02x" (Char.code c))
        | '\x80'..'\xff' when not pass_8bit ->
          Buffer.add_string buf (Printf.sprintf "\\x%02x" (Char.code c))
        | '"' -> Buffer.add_string buf "\\\""
        | c -> Buffer.add_char buf c);
      Buffer.contents buf
    in
    begin match field.pbf_default with
    | Some [%expr true]  -> Format.fprintf fmt " [default=true]"
    | Some [%expr false] -> Format.fprintf fmt " [default=false]"
#if OCAML_VERSION < (4, 03, 0)
    | Some { pexp_desc = Pexp_constant (Const_int i) } ->
#else
    | Some { pexp_desc = Pexp_constant (Pconst_integer (sn, _)) } ->
      let i = int_of_string sn in 
#endif
      Format.fprintf fmt " [default=%d]" i
    | Some { pexp_desc = Pexp_constant (Pconst_string (s, _)) } ->
      Format.fprintf fmt " [default=\"%s\"]" (escape ~pass_8bit:true s)
    | Some [%expr Bytes.of_string [%e? { pexp_desc = Pexp_constant (Pconst_string (s, _)) }]] ->
      Format.fprintf fmt " [default=\"%s\"]" (escape ~pass_8bit:false s)
    | Some { pexp_desc = Pexp_construct ({ txt = Lident n }, _) }
    | Some { pexp_desc = Pexp_variant (n, _) } ->
      Format.fprintf fmt " [default=%s_tag]" n
    | None -> ()
    | Some { pexp_loc } -> raise (Error (Pberr_ocaml_expr pexp_loc))
    end;
    Format.fprintf fmt ";"
  in
  begin match fields with
  | [{ pbf_type = Pbt_variant _ } as field] ->
    write_field field
  | ({ pbf_type = Pbt_variant _ } as field) :: fields ->
    write_field field;
    Format.fprintf fmt "@,@[<v 2>oneof value {";
    List.iter (write_field ~omit_recurrence:true) fields;
    Format.fprintf fmt "@]@,}"
  | _ -> List.iter write_field fields
  end;
  Format.fprintf fmt "@]@,}@,"

let protoc_files: (string, Format.formatter) Hashtbl.t = Hashtbl.create 16

let parse_options ~options ~path type_decls =
  let protoc        = ref None
  and protoc_import = ref []
  in
  options |> List.iter (fun (name, expr) ->
    match name with
    | "protoc" ->
      let protoc_filename =
        match expr with
        | [%expr protoc] -> (String.concat "." path) ^ ".protoc"
        | _              -> Ppx_deriving.Arg.(get_expr ~deriver string) expr
      in
      let source_path = expr.pexp_loc.loc_start.Lexing.pos_fname in
      let protoc_path =
        Filename.concat (Filename.dirname source_path) protoc_filename in
      protoc := Some protoc_path
    | "protoc_import" ->
      protoc_import := !protoc_import @ Ppx_deriving.Arg.(get_expr ~deriver (list string)) expr
    | _ -> raise_errorf ~loc:expr.pexp_loc "%s does not support option %s" deriver name);
  match !protoc with
  | Some protoc_path ->
    let fmt =
      try
        Hashtbl.find protoc_files protoc_path
      with Not_found ->
        let protoc_file = open_out protoc_path in
        let protoc_formatter = Format.formatter_of_out_channel protoc_file in
        Format.fprintf protoc_formatter
          "@[<v>// protoc file autogenerated from OCaml type definitions@,";
        Format.fprintf protoc_formatter "package %s;@," (String.concat "." path);
        at_exit (fun () -> Format.fprintf protoc_formatter "@]@?");
        Hashtbl.add protoc_files protoc_path protoc_formatter;
        protoc_formatter
    in
    List.iter (write_protoc ~fmt ~path ~import:!protoc_import) type_decls
  | None -> ()

let () =
  Ppx_deriving.(register (create "protobuf"
    ~type_decl_str:(fun ~options ~path type_decls ->
      parse_options ~options ~path type_decls;
      [Str.value Recursive (List.concat (List.map (str_of_type ~options ~path) type_decls))])
    ~type_decl_sig:(fun ~options ~path type_decls ->
      parse_options ~options ~path type_decls;
      List.concat (List.map (sig_of_type ~options ~path) type_decls))
    ()
  ))
