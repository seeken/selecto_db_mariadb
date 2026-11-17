#!/usr/bin/env bash
set -euo pipefail
backend=$1
apt-get update -qq
apt-get install -y --no-install-recommends erlang-base erlang-dev erlang-parsetools erlang-ssl erlang-public-key erlang-inets erlang-tools erlang-xmerl erlang-syntax-tools build-essential curl unzip ca-certificates
curl -fsSL https://github.com/elixir-lang/elixir/releases/download/v1.18.4/elixir-otp-25.zip -o /tmp/elixir.zip
mkdir /tmp/elixir; unzip -q /tmp/elixir.zip -d /tmp/elixir
export LANG=C.UTF-8 LC_ALL=C.UTF-8
export PATH=/tmp/elixir/bin:$PATH ERL_FLAGS='+S 2:2' MIX_ENV=prod HEX_CACERTS_PATH=/etc/ssl/certs/ca-certificates.crt
mix local.hex --force
mix local.rebar --force
cp -a /core /tmp/selecto
repo=$(basename "$(find /source -maxdepth 1 -name mix.exs -printf '%h')")
# Validate the declared public range against the pinned current core BEFORE
# choosing local paths. Path overrides alone would conceal an invalid range.
if [[ $backend != core ]]; then
 cp -a /source /tmp/adapter
 cd /tmp/adapter
 SELECTO_ECOSYSTEM_USE_LOCAL=0 elixir -e '
 Mix.start()
 version=Mix.Project.in_project(:selecto, "/tmp/selecto", fn _ -> Mix.Project.config()[:version] end)
 Code.compile_file("mix.exs")
 dep=Enum.find(Mix.Project.config()[:deps], &(elem(&1,0)==:selecto))
 req=elem(dep,1)
 unless is_binary(req) and Version.match?(version,req), do: raise("declared core range rejects pinned core")
 IO.puts("CORE_RANGE_OK #{version} #{req}")
 ' | tee /evidence/version-range.txt
fi
mkdir /tmp/consumer; cd /tmp/consumer
app=selecto
if [[ $backend != core ]]; then app=$(sed -n 's/.*app: :\([a-z_]*\).*/\1/p' /tmp/adapter/mix.exs | head -1); fi
cat > mix.exs <<EX
 defmodule Selected.MixProject do
 use Mix.Project
 def project, do: [app: :selected_consumer, version: "0.1.0", deps: [{:selecto,path: "/tmp/selecto",override: true}$(if [[ $backend != core ]]; then printf ',{:%s,path: "/tmp/adapter"}' "$app"; fi)]]
 def application, do: [extra_applications: [:logger]]
 end
EX
export SELECTO_ECOSYSTEM_USE_LOCAL=1 SELECTO_ECOSYSTEM_SELECTO_PATH=/tmp/selecto
mix deps.get
mix deps.tree > /evidence/production-dependencies.txt
mix compile
mix run /source/ci/single-backend/query.exs "$backend" | tee /evidence/query.txt
for driver in postgrex myxql exqlite duckdbex tds mongodb_driver xandra; do
 allowed=no
 case "$backend:$driver" in postgresql:postgrex|mysql:myxql|mariadb:myxql|sqlite:exqlite|duckdb:duckdbex) allowed=yes;; esac
 if [[ $allowed = no ]] && [[ -d deps/$driver ]]; then echo "foreign driver $driver"; exit 1; fi
done
bash /source/ci/single-backend/system-inventory "$backend"
find -L _build/prod/lib -name '*.so' -print > /evidence/native-assets.txt
du -sbL _build/prod/lib > /evidence/compiled-prefix-bytes.txt

# Full package tests are a separate development gate; this job deliberately
# proves only the production dependency closure and the public query above.
if [[ $backend != core ]]; then
 cd /tmp/adapter
 SELECTO_ECOSYSTEM_USE_LOCAL=0 mix hex.build > /evidence/package-build.txt 2>&1
fi
