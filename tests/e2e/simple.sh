#!/usr/bin/env bash

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

assert-body-contains() {
	local url="$1"
	local expected_content="$2"

	curl --silent --show-error "$url" | grep --fixed-strings --quiet -- "$expected_content"
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
	if kill -0 "$mirror_intel_pid" 2>/dev/null; then
		kill "$mirror_intel_pid"
	fi
	wait "$mirror_intel_pid" 2>/dev/null || true
}

base_url="http://localhost:8000"
rocket_toml_path="${ROCKET_TOML_PATH:-Rocket.toml}"

test -r "$rocket_toml_path"

RUST_LOG_FORMAT=plain \
	RUST_LOG=info \
	ROCKET_TOML_PATH="$rocket_toml_path" \
	mirror-intel &
mirror_intel_pid="$!"
trap cleanup EXIT

wait-for-connection "$base_url/metrics"

assert-status GET "$base_url/metrics" 200
assert-body-contains "$base_url/metrics" "resolve_counter"

assert-status GET "$base_url/pytorch-wheels/" 200
assert-body-contains "$base_url/pytorch-wheels/" "No route for pytorch-wheels."

assert-status HEAD "$base_url/pytorch-wheels/torch/" 200

assert-status GET "$base_url/pytorch-wheels/torch/?mirror_intel_e2e=1" 302
assert-location \
	"$base_url/pytorch-wheels/torch/?mirror_intel_e2e=1" \
	"https://download.pytorch.org/whl/torch?mirror_intel_e2e=1"

# shellcheck disable=SC2154
touch "$out"
