# runtime.yaml: config/production-defaults.yaml with the two identity placeholders bound to the
# deployment values: the identity contract refuses an unresolved "${...}", and the local adapter
# requires signaling.identity.* to equal the contract. Run with -j (raw, no added newline).
# Arguments: --rawfile defaults <production-defaults.yaml> $issuer $audience.
# Values are written as JSON strings, which are valid YAML double-quoted scalars.
def bind($key; $value):
  . as $lines
  | [range(0; $lines | length) | select($lines[.] | test("^    " + $key + ": \"\\$\\{"))] as $at
  | if ($at | length) != 1 then error("runtime.jq: expected one placeholder line for " + $key)
    else .[$at[0]] = "    " + $key + ": " + ($value | tojson)
    end;

($defaults | split("\n"))
| bind("issuer"; $issuer)
| bind("audience"; $audience)
| ["# LOCAL_TEST_ONLY / NOT_QUALIFIED. Rendered from config/production-defaults.yaml by provisioner/lib/render.sh."]
  + map(select(startswith("#") | not))
| join("\n")
| if test("\\$\\{") then error("runtime.jq: unresolved placeholder left") else . end
