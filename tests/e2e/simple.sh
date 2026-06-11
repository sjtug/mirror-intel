#!/usr/bin/env bash
set -euo pipefail

wait-for-connection() {
	local url="$1"
	timeout 10s \
		retry --until=success --delay "1" -- \
		curl --silent --show-error --fail --output /dev/null "$url"
}

assert-status() {
	local method="$1"
	local url="$2"
	local expected_status="$3"
	local status

	status="$(
		curl \
			--silent --show-error \
			--request "$method" \
			--output /dev/null \
			--write-out '%{http_code}' \
			"$url"
	)"

	test "$status" = "$expected_status"
}

wait-for-status() {
	local method="$1"
	local url="$2"
	local expected_status="$3"
	local status
	local deadline

	deadline=$((SECONDS + 20))
	while true; do
		status="$(
			curl \
				--silent --show-error \
				--request "$method" \
				--output /dev/null \
				--write-out '%{http_code}' \
				"$url" || true
		)"

		if test "$status" = "$expected_status"; then
			return 0
		fi
		if test "$SECONDS" -ge "$deadline"; then
			echo "expected $method $url to return $expected_status, got $status" >&2
			return 1
		fi
		sleep 1
	done
}

assert-body-contains() {
	local url="$1"
	local expected_content="$2"

	curl --silent --show-error "$url" | grep --fixed-strings --quiet -- "$expected_content"
}

wait-for-body-contains() {
	local url="$1"
	local expected_content="$2"
	local deadline

	deadline=$((SECONDS + 20))
	while true; do
		if curl --silent --show-error "$url" | grep --fixed-strings --quiet -- "$expected_content"; then
			return 0
		fi
		if test "$SECONDS" -ge "$deadline"; then
			echo "expected $url body to contain $expected_content" >&2
			return 1
		fi
		sleep 1
	done
}

assert-location() {
	local url="$1"
	local expected_location="$2"
	local location

	location="$(
		curl --silent --show-error --dump-header - --output /dev/null "$url" |
			grep --ignore-case '^location:' |
			head --lines 1 |
			tr -d '\r' |
			sed --quiet --expression 's/^[Ll]ocation: //p'
	)"

	test "$location" = "$expected_location"
}

cleanup() {
	if test -n "${mirror_intel_pid:-}" && kill -0 "$mirror_intel_pid" 2>/dev/null; then
		kill "$mirror_intel_pid"
		wait "$mirror_intel_pid" 2>/dev/null || true
	fi
	if test -n "${fake_services_pid:-}" && kill -0 "$fake_services_pid" 2>/dev/null; then
		kill "$fake_services_pid"
		wait "$fake_services_pid" 2>/dev/null || true
	fi
	if test -n "${nix_serve_pid:-}" && kill -0 "$nix_serve_pid" 2>/dev/null; then
		kill "$nix_serve_pid"
		wait "$nix_serve_pid" 2>/dev/null || true
	fi
}

base_url="http://localhost:8000"
upstream_url="http://127.0.0.1:18080"
nix_store_url="http://127.0.0.1:18082"
s3_url="http://127.0.0.1:18081"
rocket_toml_path="${ROCKET_TOML_PATH:-Rocket.toml}"
script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
mirror_intel_pid=""
fake_services_pid=""
nix_serve_pid=""

test -r "$rocket_toml_path"
test -e "$NIX_CACHE_FIXTURE"
mkdir -p buffer
nix_store_nar="$PWD/nix-cache-fixture.nar"
nix_store_db_dump="$PWD/nix-store-db.dump"
nix-store --dump "$NIX_CACHE_FIXTURE" > "$nix_store_nar"
nix_store_nar_hash="$(sha256sum "$nix_store_nar" | cut --delimiter ' ' --fields 1)"
nix_store_nar_size="$(wc --bytes < "$nix_store_nar" | tr --delete '[:space:]')"
printf '%s\n%s\n%s\n\n0\n' \
	"$NIX_CACHE_FIXTURE" \
	"$nix_store_nar_hash" \
	"$nix_store_nar_size" > "$nix_store_db_dump"

export NIX_STATE_DIR="$PWD/nix-state"
export NIX_LOG_DIR="$PWD/nix-log"
export NIX_CONF_DIR="$PWD/nix-conf"
mkdir -p "$NIX_STATE_DIR/profiles" "$NIX_STATE_DIR/gcroots" "$NIX_LOG_DIR" "$NIX_CONF_DIR"
nix-store --load-db < "$nix_store_db_dump"

python3 "$script_dir/fake_services.py" &
fake_services_pid="$!"
trap cleanup EXIT

nix-serve --host 127.0.0.1 --port 18082 --quiet &
nix_serve_pid="$!"

wait-for-connection "$upstream_url/health"
wait-for-connection "$nix_store_url/nix-cache-info"
wait-for-connection "$s3_url/health"

nix_cache_basename="$(basename -- "$NIX_CACHE_FIXTURE")"
nix_cache_narinfo="${nix_cache_basename%%-*}.narinfo"
nix_cache_nar_path="$(
	curl --silent --show-error --fail "$nix_store_url/$nix_cache_narinfo" |
		sed --quiet --expression 's/^URL: //p' |
		head --lines 1
)"
test -n "$nix_cache_nar_path"

export AWS_ACCESS_KEY_ID=fake
export AWS_SECRET_ACCESS_KEY=fake
export AWS_EC2_METADATA_DISABLED=true

RUST_LOG_FORMAT=plain \
	RUST_LOG=info \
	ROCKET_TOML_PATH="$rocket_toml_path" \
	mirror-intel &
mirror_intel_pid="$!"

wait-for-connection "$base_url/metrics"

assert-status GET "$base_url/metrics" 200
assert-body-contains "$base_url/metrics" "resolve_counter"

assert-status GET "$base_url/pytorch-wheels/" 200
assert-body-contains "$base_url/pytorch-wheels/" "No route for pytorch-wheels."

assert-status GET "$base_url/pytorch-wheels/torch/?mirror_intel_e2e=1" 302
assert-location \
	"$base_url/pytorch-wheels/torch/?mirror_intel_e2e=1" \
	"$upstream_url/whl/torch?mirror_intel_e2e=1"

assert-status GET "$base_url/pytorch-wheels/cu130/torch/?mirror_intel_e2e=1" 302
assert-location \
	"$base_url/pytorch-wheels/cu130/torch/?mirror_intel_e2e=1" \
	"$upstream_url/whl/cu130/torch?mirror_intel_e2e=1"

wait-for-status GET "$base_url/pytorch-wheels/cu130/torch/" 302
wait-for-body-contains "$base_url/pytorch-wheels/cu130/torch/" "cu130 torch cached directory index"

assert-status GET "$base_url/pytorch-wheels/cu130/torch/" 200
assert-body-contains "$base_url/pytorch-wheels/cu130/torch/" "cu130 torch cached directory index"
assert-body-contains "$s3_url/bucket/pytorch-wheels/cu130/torch" "cu130 torch cached directory index"
assert-status HEAD "$base_url/pytorch-wheels/cu130/torch/" 301

assert-status GET "$base_url/nix-channels/store/nix-cache-info" 200
assert-body-contains "$base_url/nix-channels/store/nix-cache-info" "StoreDir: /nix/store"

assert-status GET "$base_url/nix-channels/store/$nix_cache_narinfo?mirror_intel_e2e=1" 302
assert-location \
	"$base_url/nix-channels/store/$nix_cache_narinfo?mirror_intel_e2e=1" \
	"$nix_store_url/$nix_cache_narinfo?mirror_intel_e2e=1"

assert-status GET "$base_url/nix-channels/store/$nix_cache_narinfo" 200
assert-body-contains \
	"$base_url/nix-channels/store/$nix_cache_narinfo" \
	"StorePath: $NIX_CACHE_FIXTURE"
wait-for-body-contains \
	"$s3_url/bucket/nix-channels/store/$nix_cache_narinfo" \
	"StorePath: $NIX_CACHE_FIXTURE"
assert-body-contains \
	"$base_url/nix-channels/store/$nix_cache_narinfo" \
	"StorePath: $NIX_CACHE_FIXTURE"

assert-status GET "$base_url/nix-channels/store/$nix_cache_nar_path" 200
wait-for-status GET "$s3_url/bucket/nix-channels/store/$nix_cache_nar_path" 200
assert-status GET "$base_url/nix-channels/store/$nix_cache_nar_path" 200

# shellcheck disable=SC2154
touch "$out"
