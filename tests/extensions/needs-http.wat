;; A part that needs a capability Pondra 1.0 doesn't offer (`pondra:http/http@1.0.0`), as an extension built for a later
;; Pondra may: the host refuses it by the import's name (E5). Made by tools/extension_fixture.sh (wasm-tools parse).
(component
  (import "pondra:http/http@1.0.0" (instance
    (export "fetch" (func (param "url" string) (result string)))
  ))
)
