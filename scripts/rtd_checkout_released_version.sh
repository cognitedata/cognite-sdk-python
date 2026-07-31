# Read the Docs builds the 'latest' version from the tip of master, which holds commits
# that are merged but not yet released. Check out the most recent release tag instead so
# that the published documentation never describes an unreleased version of the SDK.
#
# Meant to run as a post_checkout job in .readthedocs.yaml. Tag builds and pull request
# previews are left alone: they should document the ref they were triggered for.
set -e

if [ -z "$READTHEDOCS_VERSION" ] || [ -z "$READTHEDOCS_VERSION_TYPE" ]; then
    echo "READTHEDOCS_VERSION and READTHEDOCS_VERSION_TYPE must be set, are we on Read the Docs?" >&2
    exit 1
fi

if [ "$READTHEDOCS_VERSION_TYPE" != "branch" ] || [ "$READTHEDOCS_VERSION" != "latest" ]; then
    echo "Nothing to do for $READTHEDOCS_VERSION_TYPE version '$READTHEDOCS_VERSION'."
    exit 0
fi

released_version=$(sed -n 's/^[[:space:]]*"\.":[[:space:]]*"\(.*\)".*/\1/p' .release-please-manifest.json)
if [ -z "$released_version" ]; then
    echo "Could not read the released version from .release-please-manifest.json." >&2
    exit 1
fi

# release-please tags releases as '<package-name>-v<version>'; match on the version alone so
# a change to the tag template does not silently leave us building unreleased code.
set +e
matching_refs=$(git ls-remote --tags origin "*v$released_version")
ls_remote_status=$?
set -e
if [ $ls_remote_status -ne 0 ]; then
    echo "Failed to list remote tags (git ls-remote exited with $ls_remote_status)." >&2
    exit $ls_remote_status
fi

tag=$(echo "$matching_refs" | sed -n 's|^[0-9a-f]*[[:space:]]*refs/tags/\([^^]*\)$|\1|p' | sed -n 1p)
if [ -z "$tag" ]; then
    # release-please creates the tag shortly after merging its release PR, so a missing tag
    # means we are building that release commit already. The next build picks up the tag.
    echo "No tag for version $released_version yet, building the current commit."
    exit 0
fi

echo "Building documentation for released version $released_version (tag $tag)."
git fetch --depth 1 origin "refs/tags/$tag:refs/tags/$tag"
git checkout --force "$tag"
