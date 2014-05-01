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
and pb_type =
| Pbt_bool
| Pbt_int
| Pbt_int32
| Pbt_int64
| Pbt_uint32
| Pbt_uint64
| Pbt_float
| Pbt_string
| Pbt_tuple   of core_type
| Pbt_variant of (int * string) list
| Pbt_nested  of core_type list * Longident.t
| Pbt_poly    of string
and pb_kind =
| Pbk_required
| Pbk_optional
| Pbk_repeated
and pb_field = {
  pbf_name  : string;
  pbf_path  : string;
  pbf_key   : int;
  pbf_enc   : pb_encoding;
  pbf_type  : pb_type;
  pbf_kind  : pb_kind;
  pbf_loc   : Location.t;
}

type pb_error =
| Pberr_attr_syntax   of Location.t * [ `Key | `Encoding | `Bare ]
| Pberr_no_key        of Location.t
| Pberr_key_invalid   of Location.t * int
| Pberr_key_duplicate of int * Location.t * Location.t
| Pberr_abstract      of type_declaration
| Pberr_wrong_ty      of core_type
| Pberr_wrong_tparm   of core_type
| Pberr_no_conversion of Location.t * pb_type * pb_encoding

exception Error of pb_error

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

let string_of_pb_encoding enc =
  match enc with
  | Pbe_varint -> "varint"
  | Pbe_zigzag -> "zigzag"
  | Pbe_bits32 -> "bits32"
  | Pbe_bits64 -> "bits64"
  | Pbe_bytes  -> "bytes"

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
  | Pbt_tuple ptyp ->
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
  | Pbe_varint | Pbe_zigzag -> "Varint"
  | Pbe_bits32 -> "Bits32"
  | Pbe_bits64 -> "Bits64"
  | Pbe_bytes  -> "Bytes"

let () =
  Location.register_error_of_exn (fun exn ->
    match exn with
    | Error (Pberr_attr_syntax (loc, attr)) ->
      let name, expectation =
        match attr with
        | `Key      -> "key", "a number, e.g. [@key 1]"
        | `Encoding -> "encoding", "one of: bool, varint, zigzag, bits32, bits64, " ^
                                   "bytes, e.g. [@encoding varint]"
        | `Bare     -> "bare", "[@bare]"
      in
      Some (errorf ~loc "@%s attribute syntax is invalid: expected %s" name expectation)
    | Error (Pberr_no_key loc) ->
      Some (errorf ~loc "Type specification must include a key attribute, e.g. int [@key 42]")
    | Error (Pberr_key_invalid (loc, key)) ->
      if key >= 19000 && key <= 19999 then
        Some (errorf ~loc "Key %d is in reserved range [19000:19999]" key)
      else
        Some (errorf ~loc "Key %d is outside of valid range [1:0x1fffffff]" key)
    | Error (Pberr_key_duplicate (key, loc1, loc2)) ->
      Some (errorf ~loc:loc1 "Key %d is already used" key
                   ~sub:[errorf ~loc:loc2 "Initially defined here"])
    | Error (Pberr_abstract { ptype_loc = loc }) ->
      Some (errorf ~loc "Type is abstract")
    | Error (Pberr_wrong_ty ({ ptyp_loc = loc } as ptyp)) ->
      Some (errorf ~loc "Type %s does not have a Protobuf mapping" (string_of_core_type ptyp))
    | Error (Pberr_wrong_tparm ({ ptyp_loc = loc } as ptyp)) ->
      Some (errorf ~loc "Type %s cannot be used as a type parameter" (string_of_core_type ptyp))
    | Error (Pberr_no_conversion (loc, kind, enc)) ->
      Some (errorf ~loc "\"%s\" is not a valid representation for %s"
                        (string_of_pb_encoding enc) (string_of_pb_type kind))
    | _ -> None)

let mangle_lid ?(suffix="") lid =
  match lid with
  | Lident s    -> Lident (s ^ suffix)
  | Ldot (p, s) -> Ldot (p, s ^ suffix)
  | Lapply _    -> assert false

let module_name loc =
  let (file, _, _) = get_pos_info loc.loc_start in
  String.capitalize (Filename.(basename (chop_suffix file ".ml")))

let pb_key_of_attrs attrs =
  match List.find (fun ({ txt }, _) -> txt = "key") attrs with
  | { loc }, PStr [{ pstr_desc = Pstr_eval ({
                        pexp_desc = Pexp_constant (Const_int key) }, _) }] ->
    if key < 1 || key > 0x1fffffff || (key >= 19000 && key <= 19999) then
      raise (Error (Pberr_key_invalid (loc, key)));
    key
  | { loc }, _ -> raise (Error (Pberr_attr_syntax (loc, `Key)))

let pb_encoding_of_attrs attrs =
  match List.find (fun ({ txt }, _) -> txt = "encoding") attrs with
  | _, PStr [{ pstr_desc = Pstr_eval ({
                pexp_desc = Pexp_ident { txt = Lident kind; loc } }, _) }] ->
    begin try
      pb_encoding_of_string kind
    with Not_found ->
      raise (Error (Pberr_attr_syntax (loc, `Encoding)))
    end
  | { loc }, _ -> raise (Error (Pberr_attr_syntax (loc, `Encoding)))

let pb_bare_of_attrs attrs =
  match List.find (fun ({ txt }, _) -> txt = "bare") attrs with
  | _, PStr [] -> ()
  | { loc }, _ -> raise (Error (Pberr_attr_syntax (loc, `Bare)))

let fields_of_ptype base_path ptype =
  let rec field_of_ptyp pbf_name pbf_path pbf_key pbf_kind ptyp =
    match ptyp with
    | [%type: [%t? arg] option ] ->
      begin match pbf_kind with
      | Pbk_required -> field_of_ptyp pbf_name pbf_path pbf_key Pbk_optional arg
      | _ -> raise (Error (Pberr_wrong_ty ptyp))
      end
    | [%type: [%t? arg] array ] | [%type: [%t? arg] list ] ->
      begin match pbf_kind with
      | Pbk_required -> field_of_ptyp pbf_name pbf_path pbf_key Pbk_repeated arg
      | _ -> raise (Error (Pberr_wrong_ty ptyp))
      end
    | { ptyp_desc = (Ptyp_tuple _ | Ptyp_var _) as desc; ptyp_attributes = attrs; ptyp_loc; } ->
      let pbf_key =
        try  pb_key_of_attrs attrs
        with Not_found ->
          match pbf_key with
          | Some k -> k
          | None -> raise (Error (Pberr_no_key ptyp_loc))
      in
      let pbf_type =
        match desc with
        | Ptyp_tuple _ -> Pbt_tuple ptyp
        | Ptyp_var var -> Pbt_poly var
        | _ -> assert false
      in
      { pbf_name; pbf_key; pbf_kind; pbf_path; pbf_type;
        pbf_enc  = Pbe_bytes;
        pbf_loc  = ptyp_loc; }
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
          | Pbt_string | Pbt_tuple _ | Pbt_poly _ -> Pbe_bytes
          | Pbt_nested _ ->
            begin
              try  pb_bare_of_attrs attrs; Pbe_varint
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
          pbf_loc = ptyp_loc; }
      | _ ->
        raise (Error (Pberr_no_conversion (ptyp_loc, pbf_type, pbf_enc)))
      end
    | { ptyp_desc = Ptyp_alias (ptyp', _) } ->
      field_of_ptyp pbf_name pbf_path pbf_key pbf_kind ptyp'
    | ptyp -> raise (Error (Pberr_wrong_ty ptyp))
  in
  let fields =
    match ptype with
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some { ptyp_desc = Ptyp_tuple ptyps } } ->
      ptyps |> List.mapi (fun i ptyp ->
        field_of_ptyp (Printf.sprintf "elem_%d" i) (Printf.sprintf "%s/%d" base_path i)
                      (Some (i + 1)) Pbk_required ptyp)
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some ptyp } ->
      [field_of_ptyp "alias" base_path (Some 1) Pbk_required ptyp]
    | { ptype_kind = Ptype_abstract; ptype_manifest = None } ->
      raise (Error (Pberr_abstract ptype))
    | { ptype_kind = Ptype_record fields } ->
      fields |> List.mapi (fun i { pld_name = { txt = name }; pld_type; } ->
        field_of_ptyp ("field_" ^ name) (Printf.sprintf "%s.%s" base_path name)
                      None Pbk_required pld_type)
    | { ptype_kind = Ptype_variant constrs; ptype_loc } ->
      let constrs' =
        constrs |> List.map (fun ({ pcd_attributes; pcd_loc; } as pcd) ->
          let key =
            try  pb_key_of_attrs pcd_attributes
            with Not_found -> raise (Error (Pberr_no_key pcd_loc))
          in key, pcd)
      in
      constrs' |> List.iter (fun (key, { pcd_loc } as pcd) ->
        constrs' |> List.iter (fun (key', { pcd_loc = pcd_loc' } as pcd') ->
          if pcd != pcd' && key = key' then
            raise (Error (Pberr_key_duplicate (key, pcd_loc', pcd_loc)))));
      { pbf_name = "variant";
        pbf_key  = 1;
        pbf_enc  = Pbe_varint;
        pbf_type = Pbt_variant (constrs' |>
              List.map (fun (key, { pcd_name = { txt = name } }) -> key, name));
        pbf_kind = Pbk_required;
        pbf_loc  = ptype_loc;
        pbf_path = base_path; } ::
      (constrs |> ExtList.List.filter_map (fun ({ pcd_name; pcd_args; pcd_attributes; pcd_loc; }) ->
        let ptyp =
          match pcd_args with
          | []    -> None
          | [arg] -> Some arg
          | args  -> Some (Typ.tuple args)
        in
        ptyp |> Option.map (fun ptyp ->
          let key = (pb_key_of_attrs pcd_attributes) + 1 in
          field_of_ptyp (Printf.sprintf "constr_%s" pcd_name.txt)
                        (Printf.sprintf "%s.%s" base_path pcd_name.txt)
                        (Some key) Pbk_required ptyp)))
  in
  fields |> List.iter (fun field ->
    fields |> List.iter (fun field' ->
      if field != field' && field.pbf_key = field'.pbf_key then
        raise (Error (Pberr_key_duplicate (field.pbf_key, field'.pbf_loc, field.pbf_loc)))));
  fields

let rec derive_reader fields ptype =
  let rec mk_tuple_readers fields k =
    match fields with
    | { pbf_type = Pbt_tuple ty; pbf_name; pbf_path; } :: rest ->
      (* Manufacture a structure just for this tuple *)
      let ptype = Type.mk ~manifest:ty (mkloc pbf_name !default_loc) in
      Exp.let_ Nonrecursive [derive_reader (fields_of_ptype pbf_path ptype) ptype]
               (mk_tuple_readers rest k)
    | _ :: rest -> mk_tuple_readers rest k
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
  let mk_reader { pbf_name; pbf_path; pbf_enc; pbf_type; } =
    let value =
      let ident = Exp.ident (lid ("Protobuf.Decoder." ^ (string_of_pb_encoding pbf_enc))) in
      [%expr [%e ident] decoder]
    in
    let overflow =
      [%expr Protobuf.Decoder.Failure (Protobuf.Decoder.Overflow [%e str pbf_path])]
    in
    match pbf_type, pbf_enc with
    (* bool *)
    | Pbt_bool, Pbe_varint ->
      [%expr Protobuf.bool_of_int64 [%e overflow] [%e value]]
    (* int *)
    | Pbt_int, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Protobuf.int_of_int64 [%e overflow] [%e value]]
    | Pbt_int, Pbe_bits32 ->
      [%expr Protobuf.int_of_int32 [%e overflow] [%e value]]
    (* int32 *)
    | Pbt_int32, Pbe_bits32 -> value
    | Pbt_int32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Protobuf.int32_of_int64 [%e overflow] [%e value]]
    (* int64 *)
    | Pbt_int64, (Pbe_varint | Pbe_zigzag | Pbe_bits64) -> value
    | Pbt_int64, Pbe_bits32 ->
      [%expr Int64.of_int32 [%e value]]
    (* uint32 *)
    | Pbt_uint32, Pbe_bits32 ->
      [%expr Uint32.of_int32 [%e value]]
    | Pbt_uint32, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      [%expr Uint32.of_int32 (Protobuf.int32_of_int64 [%e overflow] [%e value])]
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
    (* tuple *)
    | Pbt_tuple _, Pbe_bytes ->
      let ident = evar (pbf_name ^ "_from_protobuf") in
      [%expr [%e ident] (Protobuf.Decoder.nested decoder)]
    (* variant *)
    | Pbt_variant _, Pbe_varint -> value
    (* poly *)
    | Pbt_poly var, Pbe_bytes ->
      [%expr [%e evar ("poly_" ^ var)] (Protobuf.Decoder.nested decoder)]
    | _ -> assert false
  in
  let rec mk_field_cases fields =
    match fields with
    | { pbf_key; pbf_name; pbf_enc; pbf_type; pbf_kind; pbf_path } as field :: rest ->
      let updated =
        match pbf_kind with
        | Pbk_required | Pbk_optional ->
          [%expr Some [%e mk_reader field]]
        | Pbk_repeated ->
          [%expr [%e mk_reader field] :: ![%e evar pbf_name]]
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
    let { pbf_path } = fields |> List.find (fun { pbf_name = pbf_name' } -> pbf_name' = pbf_name) in
    match ptyp with
    | [%type: [%t? _] option] ->
      [%expr ![%e evar pbf_name]]
    | [%type: [%t? _] list] ->
      [%expr List.rev (![%e evar pbf_name])]
    | [%type: [%t? _] array] ->
      [%expr Array.of_list (List.rev (![%e evar pbf_name]))]
    | { ptyp_desc = (Ptyp_constr _ | Ptyp_tuple _ | Ptyp_var _); } ->
      [%expr
        match ![%e evar pbf_name] with
        | None   -> raise Protobuf.Decoder.(Failure (Missing_field [%e str pbf_path]))
        | Some v -> v
      ]
    | _ -> assert false
  in
  let constructor =
    match ptype with
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some { ptyp_desc = Ptyp_tuple ptyps } } ->
      Exp.tuple (List.mapi (fun i ptyp ->
        construct_ptyp (Printf.sprintf "elem_%d" i) ptyp) ptyps)
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some ptyp } ->
      construct_ptyp "alias" ptyp
    | { ptype_kind = Ptype_abstract; ptype_manifest = None } ->
      assert false
    | { ptype_kind = Ptype_record fields; } ->
      Exp.record (List.mapi (fun i { pld_name; pld_type; } ->
        lid pld_name.txt, construct_ptyp ("field_" ^ pld_name.txt) pld_type) fields) None
    | { ptype_kind = Ptype_variant constrs; ptype_name; ptype_loc } ->
      let with_arg =
        constrs |> ExtList.List.filter_map (fun pcd ->
          match pcd with
          | { pcd_args = [] }      -> None
          | { pcd_name = { txt } } -> Some txt)
      in
      let rec mk_variant_cases constrs =
        match constrs with
        | { pcd_name = { txt = name }; pcd_args; pcd_attributes } :: rest ->
          let pkey  = [%pat? Some [%p Pat.constant (Const_int64
                                (Int64.of_int (pb_key_of_attrs pcd_attributes)))]] in
          let pargs = with_arg |> List.map (fun name' ->
                        if name = name' then [%pat? Some arg] else [%pat? None]) in
          begin match pcd_args with
          | [] ->
            Exp.case (ptuple (pkey :: List.map (fun _ -> [%pat? None]) with_arg))
                     (constr name [])
          | [arg] ->
            Exp.case (ptuple (pkey :: pargs))
                     (Exp.construct (lid name) (Some [%expr arg]))
          | args' -> (* Annoying constructor corner case *)
            let pargs', eargs' =
              List.mapi (fun i _ ->
                let name = Printf.sprintf "a%d" i in pvar name, evar name) args' |>
              List.split
            in
            Exp.case (ptuple (pkey :: pargs))
                     [%expr let [%p ptuple pargs'] = arg in [%e constr name eargs']]
          end :: mk_variant_cases rest
        | [] ->
          let name = (module_name ptype_loc) ^ "." ^ ptype_name.txt in
          [Exp.case [%pat? _] [%expr raise Protobuf.Decoder.
                                (Failure (Malformed_variant [%e str name]))]]
      in
      Exp.match_ (tuple ([%expr !variant] ::
                         List.map (fun name -> [%expr ![%e evar ("constr_" ^ name)]]) with_arg))
                 (mk_variant_cases constrs)
  in
  let rec mk_poly tvars k =
    match tvars with
    | (Some { txt }, _) :: rest ->
      [%expr fun [%p pvar ("poly_" ^ txt)] -> [%e mk_poly rest k]]
    | (None, _) :: rest -> mk_poly rest k
    | [] -> k
  in
  let read =
    [%expr let rec read () = [%e matcher] in read (); [%e constructor]] |>
    mk_cells fields |>
    mk_tuple_readers fields
  in
  Vb.mk (pvar (ptype.ptype_name.txt ^ "_from_protobuf"))
        (mk_poly ptype.ptype_params [%expr fun decoder -> [%e read]])

let derive_reader_bare fields ptype =
  match ptype with
  | { ptype_kind = Ptype_variant constrs; ptype_name; ptype_loc } ->
    if List.for_all (fun { pcd_args } -> pcd_args = []) constrs then
      let rec mk_variant_cases constrs =
        match constrs with
        | { pcd_name = { txt = name }; pcd_attributes } :: rest ->
          (Exp.case (Pat.constant (Const_int64
                      (Int64.of_int (pb_key_of_attrs pcd_attributes))))
                    (constr name [])) :: mk_variant_cases rest
        | [] ->
          let name = (module_name ptype_loc) ^ "." ^ ptype_name.txt in
          [Exp.case [%pat? _] [%expr raise Protobuf.Decoder.
                                (Failure (Malformed_variant [%e str name]))]]
      in
      let matcher =
        Exp.match_ [%expr Protobuf.Decoder.varint decoder]
                   (mk_variant_cases constrs) in
      Some (Vb.mk (pvar (ptype.ptype_name.txt ^ "_from_protobuf_bare"))
                  [%expr fun decoder -> [%e matcher]])
    else None
  | _ -> None

let derive item =
  match item with
  | { pstr_desc = Pstr_type ty_decls } as item ->
    let derived =
      ty_decls |>
      List.map (fun ({ ptype_name = { txt = name }; ptype_loc } as ptype) ->
        let fields = fields_of_ptype ((module_name ptype_loc) ^ "." ^ name) ptype in
        [derive_reader fields ptype] @
        (Option.map_default (fun x -> [x]) [] (derive_reader_bare fields ptype))) |>
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
