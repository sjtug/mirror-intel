#!/usr/bin/env bash
set -euo pipefail

wait-for-connection() {
	local url="$1"
	timeout 10s \
		retry --until=success --delay "1" -- \
		curl --silent --show-error --fail --output /dev/null "$url" || {
			echo "FAIL wait-for-connection: url=$url" >&2
			return 1
		}
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
			"$url" || true
	)"

	if test "$status" != "$expected_status"; then
		echo "FAIL assert-status: method=$method url=$url expected=$expected_status actual=$status" >&2
		return 1
	fi
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
			echo "FAIL wait-for-status: method=$method url=$url expected=$expected_status actual=$status" >&2
			return 1
		fi
		sleep 1
	done
}

assert-body-contains() {
	local url="$1"
	local expected_content="$2"

	if ! curl --silent --show-error "$url" | grep --fixed-strings --quiet -- "$expected_content"; then
		echo "FAIL assert-body-contains: url=$url expected-substring=$expected_content" >&2
		return 1
	fi
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
			echo "FAIL wait-for-body-contains: url=$url expected-substring=$expected_content" >&2
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
			sed --quiet --expression 's/^[Ll]ocation: //p' || true
	)"

	if test "$location" != "$expected_location"; then
		echo "FAIL assert-location: url=$url expected=$expected_location actual=$location" >&2
		return 1
	fi
}

assert-status-location() {
	local method="$1"
	local url="$2"
	local expected_status="$3"
	local expected_location="$4"
	local status
	local location
	local headers

	headers="$(mktemp)"
	status="$(
		curl \
			--silent --show-error \
			--request "$method" \
			--dump-header "$headers" \
			--output /dev/null \
			--write-out '%{http_code}' \
			"$url" || true
	)"
	location="$(
		grep --ignore-case '^location:' "$headers" |
			head --lines 1 |
			tr -d '\r' |
			sed --quiet --expression 's/^[Ll]ocation: //p' || true
	)"
	rm -f "$headers"

	if test "$status" != "$expected_status"; then
		echo "FAIL assert-status-location: method=$method url=$url expected-status=$expected_status actual-status=$status expected-location=$expected_location actual-location=$location" >&2
		return 1
	fi

	if test "$location" != "$expected_location"; then
		echo "FAIL assert-status-location: method=$method url=$url expected-status=$expected_status actual-status=$status expected-location=$expected_location actual-location=$location" >&2
		return 1
	fi
}

assert-upstream-count() {
	local method="$1"
	local path="$2"
	local expected_count="$3"
	local count

	count="$(
		curl \
			--silent --show-error --get \
			--data-urlencode "method=$method" \
			--data-urlencode "path=$path" \
			"$upstream_url/__count" || true
	)"

	if test "$count" != "$expected_count"; then
		echo "FAIL assert-upstream-count: method=$method path=$path expected=$expected_count actual=$count" >&2
		return 1
	fi
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
script_dir="$(CDPATH="" cd -- "$(dirname -- "$0")" && pwd)"
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
assert-body-contains "$base_url/metrics" "s3_put_object_healthy 1"

# Generated PyPA Simple Repository indexes are served from S3 and selected by Accept.
assert-status GET "$base_url/pytorch-wheels/" 200
assert-body-contains "$base_url/pytorch-wheels/" 'href="torch/"'
if ! curl --silent --show-error --fail \
	--header 'Accept: application/vnd.pypi.simple.v1+json' \
	"$base_url/pytorch-wheels/" | grep --fixed-strings --quiet '"projects":[{"name":"torch"}]'; then
	echo "FAIL PyTorch root did not serve PEP 691 JSON" >&2
	exit 1
fi

assert-status GET "$base_url/pytorch-wheels/torch/" 200
assert-body-contains "$base_url/pytorch-wheels/torch/" "Links for torch"
assert-status GET "$base_url/pytorch-wheels/cu130/" 200
assert-status GET "$base_url/pytorch-wheels/cu130/torch/" 200
assert-body-contains "$base_url/pytorch-wheels/cu130/torch/" "Links for cu130 torch"
assert-status HEAD "$base_url/pytorch-wheels/cu130/torch/" 200

# Index misses are S3-authoritative and never probe or cache an upstream page.
assert-status GET "$base_url/pytorch-wheels/missing-cache-path/" 404
assert-upstream-count HEAD "/whl/missing-cache-path" 0
assert-upstream-count GET "/whl/missing-cache-path" 0

# Artifacts remain on-demand: the first request redirects upstream and populates S3.
assert-status-location \
	GET \
	"$base_url/pytorch-wheels/torch-0.0.1.whl" \
	302 \
	"$upstream_url/whl/torch-0.0.1.whl"
assert-upstream-count HEAD "/whl/torch-0.0.1.whl" 1
wait-for-body-contains \
	"$s3_url/bucket/pytorch-wheels/torch-0.0.1.whl" \
	"wheel cache fixture"

# Redirect-classified paths remain unconditional upstream redirects.
assert-status-location \
	GET \
	"$base_url/pytorch-wheels/missing-redirect.tar.gz" \
	301 \
	"$upstream_url/whl/missing-redirect.tar.gz"
assert-upstream-count HEAD "/whl/missing-redirect.tar.gz" 0
assert-upstream-count GET "/whl/missing-redirect.tar.gz" 0

# Legacy find-links HTML pages are not served or fetched from upstream.
assert-status HEAD "$base_url/pytorch-wheels/missing-proxy.html" 404
assert-status GET "$base_url/pytorch-wheels/missing-proxy.html?legacy=1" 404
assert-upstream-count HEAD "/whl/missing-proxy.html" 0
assert-upstream-count GET "/whl/missing-proxy.html" 0

assert-status GET "$base_url/nix-channels/store/nix-cache-info" 200
assert-body-contains "$base_url/nix-channels/store/nix-cache-info" "StoreDir: /nix/store"

assert-status-location \
	GET \
	"$base_url/nix-channels/store/$nix_cache_narinfo?mirror_intel_e2e=1" \
	302 \
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

# suppress '$out is referenced but not assigned' (Nix setup hooks assigns it)
# shellcheck disable=SC2154
touch "$out"
