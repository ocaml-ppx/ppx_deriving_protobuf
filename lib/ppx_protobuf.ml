open Ast_mapper
open Ast_helper
open Asttypes
open Parsetree
open Longident
open Location

type pb_encoding =
| Pbe_varint
| Pbe_zigzag
| Pbe_bits32
| Pbe_bits64
| Pbe_bytes
and pb_kind =
| Pbk_int
| Pbk_int32
| Pbk_int64
| Pbk_uint32
| Pbk_uint64
| Pbk_float
| Pbk_string
| Pbk_nested of pb_field list
and pb_field = {
  pbf_name  : string;
  pbf_key   : int;
  pbf_enc   : pb_encoding;
  pbf_kind  : pb_kind;
}

type pb_error =
| Pberr_wrong_ty of Location.t
| Pberr_attr_syntax of Location.t
| Pberr_no_key of Location.t
| Pberr_no_conversion of Location.t * pb_kind * pb_encoding

exception Error of pb_error

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

let rec string_of_pb_kind kind =
  match kind with
  | Pbk_int      -> "int"
  | Pbk_int32    -> "Int32.t"
  | Pbk_int64    -> "Int64.t"
  | Pbk_uint32   -> "Uint32.t"
  | Pbk_uint64   -> "Uint64.t"
  | Pbk_float    -> "float"
  | Pbk_string   -> "string"
  | Pbk_nested n -> assert false

let () =
  Location.register_error_of_exn (fun exn ->
    match exn with
    | Error (Pberr_wrong_ty loc) ->
      Some (error ~loc "Type does not have a Protobuf mapping")
    | Error (Pberr_attr_syntax loc) ->
      Some (error ~loc "Attribute syntax is invalid")
    | Error (Pberr_no_key loc) ->
      Some (error ~loc "Field must include a key number, e.g. [@key 42]")
    | Error (Pberr_no_conversion (loc, kind, enc)) ->
      Some (error ~loc (Printf.sprintf "`%s' is not a valid representation for %s"
                                       (string_of_pb_encoding enc)
                                       (string_of_pb_kind kind)))
    | _ -> None)

let lid s = mknoloc (parse s)

let pb_key_of_attrs attrs =
  match List.find (fun ({ txt }, _) -> txt = "key") attrs with
  | _, PStr [{ pstr_desc = Pstr_eval ({
                pexp_desc = Pexp_constant (Const_int key) }, _) }] -> key
  | { loc }, _ -> raise (Error (Pberr_attr_syntax loc))

let pb_encoding_of_attrs attrs =
  match List.find (fun ({ txt }, _) -> txt = "encoding") attrs with
  | _, PStr [{ pstr_desc = Pstr_eval ({
                pexp_desc = Pexp_ident { txt = Lident kind; loc } }, _) }] ->
    begin try
      pb_encoding_of_string kind
    with Not_found ->
      raise (Error (Pberr_attr_syntax loc))
    end
  | { loc }, _ -> raise (Error (Pberr_attr_syntax loc))

let fields_of_ptype ptype =
  let rec field_of_ptyp pbf_name ptyp =
    match ptyp with
    | { ptyp_desc = Ptyp_constr (lid, args); ptyp_attributes = attrs; ptyp_loc; } ->
      let pbf_kind =
        match args, lid with
        | [], { txt = Lident "int"    } -> Pbk_int
        | [], { txt = Lident "float"  } -> Pbk_float
        | [], { txt = Lident "string" } -> Pbk_string
        | [], { txt = Lident "int32" }
        | [], { txt = Ldot (Lident "Int32", "t") }  -> Pbk_int32
        | [], { txt = Lident "int64" }
        | [], { txt = Ldot (Lident "Int64", "t") }  -> Pbk_int64
        | [], { txt = Lident "uint32" }
        | [], { txt = Ldot (Lident "Uint32", "t") } -> Pbk_uint32
        | [], { txt = Lident "uint64" }
        | [], { txt = Ldot (Lident "Uint64", "t") } -> Pbk_uint64
        | _ -> raise (Error (Pberr_wrong_ty ptyp_loc))
      in
      let pbf_key =
        try  pb_key_of_attrs attrs
        with Not_found -> raise (Error (Pberr_no_key ptyp_loc))
      in
      let pbf_enc =
        try  pb_encoding_of_attrs attrs
        with Not_found ->
          match pbf_kind with
          | Pbk_int                   -> Pbe_varint
          | Pbk_int32  | Pbk_uint32   -> Pbe_bits32
          | Pbk_int64  | Pbk_uint64   -> Pbe_bits64
          | Pbk_float                 -> Pbe_bits64
          | Pbk_string | Pbk_nested _ -> Pbe_bytes
      in
      begin match pbf_kind, pbf_enc with
      | (Pbk_int | Pbk_int32 | Pbk_int64 | Pbk_uint32 | Pbk_uint64),
        (Pbe_varint | Pbe_zigzag | Pbe_bits32 | Pbe_bits64) ->
        { pbf_name; pbf_key; pbf_enc; pbf_kind; }
      | _ ->
        raise (Error (Pberr_no_conversion (ptyp_loc, pbf_kind, pbf_enc)))
      end
    | { ptyp_desc = Ptyp_alias (ptyp', _) } -> field_of_ptyp pbf_name ptyp'
    | { ptyp_loc; } -> raise (Error (Pberr_wrong_ty ptyp_loc))
  in
  match ptype with
  | { ptype_kind = Ptype_abstract; ptype_manifest = Some ptyp } ->
    [field_of_ptyp "field" ptyp]
  | { ptype_loc; } -> raise (Error (Pberr_wrong_ty ptype_loc))

let derive_reader ({ ptype_name } as ptype) =
  let rec mk_cells fields k =
    match fields with
    | field :: rest ->
      Exp.let_ Nonrecursive
               [Vb.mk (Pat.var (mknoloc field.pbf_name))
                      (Exp.apply (Exp.ident (lid "ref"))
                        ["", (Exp.construct (lid "None") None)])]
               (mk_cells rest k)
    | [] -> k
  in
  let mk_reader pbf_enc pbf_kind reader =
    let value =
      Exp.apply (Exp.ident (lid ("Protobuf.Reader." ^ (string_of_pb_encoding pbf_enc))))
                ["", reader]
    in
    match pbf_kind, pbf_enc with
    | Pbk_int, (Pbe_varint | Pbe_zigzag | Pbe_bits64) ->
      Exp.apply (Exp.ident (lid "Protobuf.Reader.int_of_int64")) ["", value]
    | Pbk_int, Pbe_bits32 ->
      Exp.apply (Exp.ident (lid "Protobuf.Reader.int_of_int64")) ["", value]
    | _ -> assert false
  in
  let rec mk_field_cases fields =
    match fields with
    | { pbf_key; pbf_name; pbf_enc; pbf_kind; } :: rest ->
      (Exp.case (Pat.construct (lid "Some")
                  (Some (Pat.tuple
                    [Pat.constant (Const_int pbf_key);
                      Pat.construct
                        (lid ("Protobuf." ^ (String.capitalize (string_of_pb_encoding pbf_enc))))
                        None])))
                (Exp.sequence
                  (Exp.apply
                    (Exp.ident (lid ":="))
                    ["", Exp.ident (lid pbf_name);
                     "", Exp.construct (lid "Some")
                      (Some (mk_reader pbf_enc pbf_kind (Exp.ident (lid "reader"))))])
                  (Exp.apply (Exp.ident (lid "read"))
                    ["", Exp.construct (lid "()") None]))) ::
      (Exp.case (Pat.construct (lid "Some")
                  (Some (Pat.tuple [Pat.constant (Const_int pbf_key);
                                    Pat.var (mknoloc "kind")])))
                (Exp.apply
                  (Exp.ident (lid "raise"))
                  ["", Exp.construct (lid "Protobuf.Reader.Error")
                    (Some (Exp.construct (lid "Protobuf.Reader.Unexpected_payload")
                      (Some (Exp.tuple
                        [Exp.constant (Const_string (ptype_name.txt, None));
                         Exp.ident (lid "kind")]))))])) ::
      mk_field_cases rest
    | [] -> []
  in
  let fields = fields_of_ptype ptype in
  let matcher =
    Exp.match_ (Exp.apply (Exp.ident (lid "Protobuf.Reader.key"))
                          ["", Exp.ident (lid "reader")])
               ((mk_field_cases fields) @
                [Exp.case (Pat.construct (lid "Some")
                            (Some (Pat.tuple [Pat.any (); Pat.var (mknoloc "kind")])))
                          (Exp.sequence
                            (Exp.apply (Exp.ident (lid "Protobuf.Reader.skip"))
                              ["", Exp.ident (lid "reader");
                               "", Exp.ident (lid "kind")])
                            (Exp.apply (Exp.ident (lid "read"))
                              ["", Exp.construct (lid "()") None]));
                 Exp.case (Pat.construct (lid "None") None)
                          (Exp.construct (lid "()") None)])
  in
  let construct_ptyp pbf_name ptyp =
    match ptyp with
    | { ptyp_desc = Ptyp_constr _; } ->
      Exp.match_ (Exp.apply (Exp.ident (lid "!"))
                            ["", Exp.ident (lid pbf_name)])
                 [Exp.case  (Pat.construct (lid "None") None)
                            (Exp.apply
                              (Exp.ident (lid "raise"))
                              ["", Exp.construct (lid "Protobuf.Reader.Error")
                                (Some (Exp.construct (lid "Protobuf.Reader.Missing_field")
                                  (Some (Exp.constant (Const_string (ptype_name.txt, None))))))]);
                  Exp.case  (Pat.construct (lid "Some") (Some (Pat.var (mknoloc "v"))))
                            (Exp.ident (lid "v"))]
    | _ -> assert false
  in
  let construct_ptype ptype =
    match ptype with
    | { ptype_kind = Ptype_abstract; ptype_manifest = Some ptyp } ->
      construct_ptyp "field" ptyp
    | _ -> assert false
  in
  let reader =
    mk_cells fields (
      Exp.let_  Recursive
                [Vb.mk  (Pat.var (mknoloc "read"))
                        (Exp.fun_ "" None (Pat.construct (lid "()") None)
                                  matcher)]
                        (Exp.sequence
                          (Exp.apply (Exp.ident (lid "read"))
                                     ["", Exp.construct (lid "()") None])
                          (construct_ptype ptype)))
  in
  Vb.mk (Pat.var (mknoloc (ptype_name.txt ^ "_from_protobuf")))
        (Exp.fun_ "" None (Pat.var (mknoloc "reader")) reader)

let derive item =
  match item with
  | { pstr_desc = Pstr_type ty_decls } as item ->
    [item; Str.value Recursive (List.map derive_reader ty_decls)]
  | _ -> assert false

let protobuf_mapper argv =
  { default_mapper with
    structure = fun mapper items ->
      let rec map_types items =
        match items with
        | { pstr_desc = Pstr_type ty_decls } as item :: rest when
            (List.exists (fun { ptype_attributes = attrs } ->
              List.exists (fun ({ txt }, _) -> txt = "protobuf") attrs) ty_decls) ->
          derive item @ map_types rest
        | item :: rest ->
          mapper.structure_item mapper item :: map_types rest
        | [] -> []
      in
      map_types items
  }

let () = run_main protobuf_mapper
