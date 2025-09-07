# Gitopia Migration Tools Makefile

.PHONY: all clean build-clone build-sync build-update build-verify install help

# Default target
all: build-clone build-sync build-update build-verify

# Build clone script
build-clone:
	@echo "Building clone script..."
	cd clone-script && go mod download && go build -o ../bin/clone ./cmd/clone

# Build sync daemon
build-sync:
	@echo "Building sync daemon..."
	cd sync-daemon && go mod download && go build -o ../bin/syncd ./cmd/syncd

# Build update script
build-update:
	@echo "Building update script..."
	cd update-script && go mod download && go build -o ../bin/update ./cmd/update

# Build verify script
build-verify:
	@echo "Building verify script..."
	cd verify-script && go mod download && go build -o ../bin/verify ./cmd/verify

# Create bin directory
bin:
	mkdir -p bin

# Build all components
build: bin build-clone build-sync build-update build-verify
	@echo "All components built successfully!"

# Install binaries to system PATH
install: build
	@echo "Installing binaries to /usr/local/bin..."
	sudo cp bin/clone /usr/local/bin/gitopia-clone
	sudo cp bin/syncd /usr/local/bin/gitopia-syncd
	sudo cp bin/update /usr/local/bin/gitopia-update
	sudo cp bin/verify /usr/local/bin/gitopia-verify
	@echo "Installation complete!"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	cd clone-script && go clean
	cd sync-daemon && go clean
	cd update-script && go clean
	cd verify-script && go clean
	@echo "Clean complete!"

# Show help
help:
	@echo "Gitopia Migration Tools"
	@echo ""
	@echo "Available targets:"
	@echo "  all          - Build all components (default)"
	@echo "  build        - Build all components with bin directory"
	@echo "  build-clone  - Build only clone script"
	@echo "  build-sync   - Build only sync daemon"
	@echo "  build-update - Build only update script"
	@echo "  build-verify - Build only verify script"
	@echo "  install      - Install binaries to system PATH"
	@echo "  clean        - Remove build artifacts"
	@echo "  help         - Show this help message"
	@echo ""
	@echo "Components:"
	@echo "  clone-script/  - Clones repos and assets (gitopia v5.1.0)"
	@echo "  sync-daemon/   - Syncs changes until upgrade (gitopia v5.1.0)"
	@echo "  update-script/ - Updates blockchain after upgrade (gitopia v6.0.0-rc.6)"
	@echo "  verify-script/ - Verifies IPFS packfiles contain all repository refs"
