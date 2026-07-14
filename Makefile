.PHONY: help setup setup-rust setup-airflow check dev-airflow build-rust

# Default target
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Setup:"
	@echo "  setup          - Install all dependencies (Rust + Airflow)"
	@echo "  setup-rust     - Install Rust toolchain and build"
	@echo "  setup-airflow  - Install Astro CLI and start Airflow locally"
	@echo ""
	@echo "Dev:"
	@echo "  dev-airflow    - Start Airflow locally via Astro CLI"
	@echo "  build-rust     - Build Rust binary (release)"
	@echo ""
	@echo "Infra:"
	@echo "  kustomize-local  - Preview kustomize output for local overlay"
	@echo "  apply-local      - Apply local overlay to current kubectl context"
	@echo ""
	@echo "Utils:"
	@echo "  check          - Check all required tools are installed"

# ─── Setup ───────────────────────────────────────────────────────────────────

setup: check setup-rust setup-airflow
	@echo ""
	@echo "Setup complete! Run 'make dev-airflow' to start Airflow."

setup-rust:
	@echo ">>> Setting up Rust..."
	@if ! command -v rustup &> /dev/null; then \
		echo "Installing rustup..."; \
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
		source "$$HOME/.cargo/env"; \
	fi
	@echo "Building reconcile binary..."
	cd reconcile && cargo build

setup-airflow:
	@echo ">>> Setting up Astro CLI..."
	@if ! command -v astro &> /dev/null; then \
		echo "Installing Astro CLI via Homebrew..."; \
		brew install astro; \
	fi
	@echo "Astro CLI ready. Run 'make dev-airflow' to start."

# ─── Dev ─────────────────────────────────────────────────────────────────────

dev-airflow:
	@echo ">>> Starting Airflow locally (Astro CLI)..."
	cd reconcile-airflow && astro dev start

build-rust:
	@echo ">>> Building Rust (release)..."
	cd reconcile && cargo build --release
	@echo "Binary: reconcile/target/release/reconcile"

# ─── Infra ───────────────────────────────────────────────────────────────────

kustomize-local:
	kubectl kustomize infra/overlays/local

apply-local:
	kubectl apply -k infra/overlays/local

# ─── Utils ───────────────────────────────────────────────────────────────────

check:
	@echo ">>> Checking required tools..."
	@command -v docker    &> /dev/null && echo "  [ok] docker"    || echo "  [missing] docker    -> https://docs.docker.com/get-docker/"
	@command -v kubectl   &> /dev/null && echo "  [ok] kubectl"   || echo "  [missing] kubectl   -> brew install kubectl"
	@command -v kustomize &> /dev/null && echo "  [ok] kustomize" || echo "  [missing] kustomize -> brew install kustomize"
	@command -v astro     &> /dev/null && echo "  [ok] astro"     || echo "  [missing] astro     -> brew install astro"
	@command -v cargo     &> /dev/null && echo "  [ok] cargo"     || echo "  [missing] cargo     -> make setup-rust"
