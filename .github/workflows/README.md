# Build and Release Workflow Documentation

## Overview

This repository uses a unified GitHub Actions workflow (`build-and-release.yml`) that handles all aspects of building, testing, and releasing the MSSQLDBSink application across multiple platforms.

## Workflow Features

### 1. Change Detection
The workflow intelligently detects whether application code has changed:
- **Pull Requests**: Only builds artifacts if code in the `src/` directory has changed
- **Push to Main**: Always builds and releases
- **Release Events**: Always builds and uploads artifacts

### 2. Comprehensive Testing
- Runs unit tests with code coverage
- Generates HTML coverage reports
- Uploads coverage to Codecov
- Posts test results and coverage as PR comments
- Tests against pre-built artifacts from main when only tests change

### 3. Automatic Version Management
- Automatically increments patch version on push to main
- Updates `<Version>` tag in the project file
- Optionally updates `<AssemblyVersion>`, `<FileVersion>`, and `<InformationalVersion>` if present
- Validates semantic versioning format

### 4. Multi-Platform Builds
Builds self-contained executables for:
- **Windows**: x64, ARM64 (`.zip` archives)
- **Linux**: x64, ARM64 (`.tar.gz` archives)
- **macOS**: ARM64 (`.zip` archives)

### 5. Pre-release Management
For pull requests:
- Creates pre-release with versioned artifacts
- Names: `v{version}-pr{number}`
- Includes all platform builds
- Automatically deleted when PR is merged

### 6. Release Management
On push to main:
- Increments version
- Builds all platform artifacts
- Deletes all pre-releases
- Creates a new full release with versioned artifacts
- Tag format: `v{version}`

On release published:
- Validates version and tag match
- Builds and uploads artifacts to the release
- Ensures version is not a pre-release version

## Workflow Jobs

### Job: check-changes
Determines if application code has changed by inspecting the `src/` directory.

**Outputs:**
- `src_changed`: `true` if application code changed, `false` otherwise

### Job: test
Runs all tests with code coverage:
- Restores dependencies
- Builds test project
- Runs tests with coverage collection
- Generates HTML coverage report
- Uploads coverage to Codecov
- Posts results as PR comment

**Artifacts:**
- `coverage-report`: HTML and XML coverage reports

### Job: increment-version
Automatically increments the patch version on push to main.

**Runs when:**
- Event is push to main
- Application code has changed

**Outputs:**
- `new_version`: The new version number (e.g., `0.1.2`)

### Job: build-windows
Builds Windows executables on Windows runner.

**Builds:**
- Windows x64
- Windows ARM64

**Artifacts:**
- `windows-executables`: `.zip` archives

### Job: build-macos
Builds macOS executables on macOS runner.

**Builds:**
- macOS ARM64 (Apple Silicon)

**Artifacts:**
- `macos-executables`: `.zip` archives

### Job: build-linux
Builds Linux executables on Ubuntu runner.

**Builds:**
- Linux x64
- Linux ARM64

**Artifacts:**
- `linux-executables`: `.tar.gz` archives

### Job: create-prerelease
Creates a pre-release for pull requests.

**Runs when:**
- Event is pull request
- Application code has changed

**Creates:**
- Pre-release with tag `v{version}-pr{number}`
- Uploads all platform artifacts with version in filename

### Job: create-release
Handles release creation when a release is published via GitHub UI.

**Runs when:**
- Event is release published

**Validates:**
- Version format is semantic versioning
- Version is NOT a pre-release version
- Tag matches the version in project file

**Uploads:**
- All platform artifacts with version in filename

### Job: cleanup-and-release
Cleans up pre-releases and creates a full release on merge to main.

**Runs when:**
- Event is push to main

**Actions:**
1. Deletes all pre-releases and their tags
2. Downloads all build artifacts
3. Creates a new release with tag `v{version}`
4. Uploads all platform artifacts with version in filename

## Environment Variables

The workflow uses the following environment variables (configured at the top of the workflow):

```yaml
DOTNET_VERSION: '10.0.x'
PROJECT_PATH: 'src/MSSQLDBSink/MSSQLDBSink.csproj'
TEST_PROJECT_PATH: 'src/MSSQLDBSink.Tests/MSSQLDBSink.Tests.csproj'
```

## Trigger Events

The workflow is triggered on:
- **Pull Requests** to `main` or `master` branches
- **Push** to `main` or `master` branches
- **Release** published events

## Artifact Naming

Artifacts are named with the following pattern:
- `MSSQLDBSink-{platform}-{arch}-{version}.{ext}`

Examples:
- `MSSQLDBSink-windows-x64-0.1.0.zip`
- `MSSQLDBSink-linux-x64-0.1.0.tar.gz`
- `MSSQLDBSink-macos-arm64-0.1.0.zip`

## Version Management

### Current Version
The current version is stored in `src/MSSQLDBSink/MSSQLDBSink.csproj`:
```xml
<Version>0.1.0</Version>
```

### Version Increment
- Automatically increments **patch** version on push to main
- Example: `0.1.0` → `0.1.1` → `0.1.2`

### Manual Version Updates
To manually update the version:
1. Edit `src/MSSQLDBSink/MSSQLDBSink.csproj`
2. Update the `<Version>` tag
3. Commit and push to main

## Testing the Workflow

### Test on a Pull Request
1. Create a branch with code changes in `src/`
2. Open a pull request
3. The workflow will:
   - Run tests with coverage
   - Post test results as a comment
   - Build all platform artifacts
   - Create a pre-release

### Test Version Increment
1. Merge a PR to main
2. The workflow will:
   - Increment the version
   - Build all platform artifacts
   - Delete pre-releases
   - Create a new release

### Test Release Published
1. Create a release via GitHub UI
2. Ensure the tag matches the version in the project file
3. The workflow will:
   - Validate version and tag
   - Build all platform artifacts
   - Upload artifacts to the release

## Troubleshooting

### Version Validation Errors
If you see version validation errors:
- Ensure the version in the project file follows semantic versioning: `MAJOR.MINOR.PATCH`
- For releases, ensure the version is NOT a pre-release version (no `-beta`, `-alpha`, etc.)
- Ensure the release tag matches the version (e.g., `v0.1.0` matches version `0.1.0`)

### Build Failures
If builds fail:
- Check the .NET version in the workflow matches your project's target framework
- Ensure all dependencies are available via NuGet
- Check platform-specific build issues in the job logs

### Test Failures
If tests fail:
- Check the test project path is correct
- Ensure test dependencies are restored
- Review test logs for specific failures

## Best Practices

1. **Always run tests locally** before pushing
2. **Use semantic versioning** for version numbers
3. **Keep pre-releases for testing** - they're automatically cleaned up on merge
4. **Review PR comments** for test and coverage results
5. **Use branch protection** to require passing tests before merge

## Coverage Requirements

The workflow tracks code coverage and displays it in PR comments:
- 🟢 Green: ≥80% coverage
- 🟡 Yellow: ≥60% coverage
- 🔴 Red: <60% coverage

Coverage reports are available as workflow artifacts for detailed analysis.
