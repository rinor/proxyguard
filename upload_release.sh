#!/bin/sh

set -e

ORG=eduvpn
PROJECT_NAME=$(basename "$(pwd)")
PROJECT_VERSION=$(grep -o 'const version = "[^"]*' version.go | cut -d '"' -f 2)
CODEBERG_API_KEY=$(cat "${XDG_CONFIG_HOME}/codeberg.org/api.key")
RELEASE_DIR="${PWD}/release"
mkdir -p "$RELEASE_DIR"

if ! command -v "tar" >/dev/null; then
    echo "please install tar for archiving the code"
    exit 1
fi

if ! command -v "curl" >/dev/null; then
    echo "please install curl for uploading to codeberg"
    exit 1
fi

if ! command -v "minisign" >/dev/null; then
    echo "please install minisign for signing the archive"
    exit 1
fi

if [ "$(git tag -l "${PROJECT_VERSION}")" ]; then
    echo "Version: ${PROJECT_VERSION} already has a tag"
    exit 1
fi

# upload to codeberg, codeberg automatically creates the tag against main
JSON_BODY="{\"tag_name\": \"${PROJECT_VERSION}\"}"

# create tag
git tag "${PROJECT_VERSION}" -m "update to ${PROJECT_VERSION}"
git push origin --tags

# create archive
git archive --prefix "${PROJECT_NAME}-${PROJECT_VERSION}/" "${PROJECT_VERSION}" | tar -xf -
tar -cJf "${RELEASE_DIR}/${PROJECT_NAME}-${PROJECT_VERSION}.tar.xz" "${PROJECT_NAME}-${PROJECT_VERSION}"

# sign
echo "signing with minisign, maybe prompted for password"
minisign -Sm "${RELEASE_DIR}/${PROJECT_NAME}-${PROJECT_VERSION}.tar.xz"

# create the release
RELEASE_ID=$(curl -s \
    -H "Authorization: token ${CODEBERG_API_KEY}" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -d "${JSON_BODY}" \
    "https://codeberg.org/api/v1/repos/${ORG}/${PROJECT_NAME}/releases" | jq .id)

# upload the artifact(s)
for F in release/*"${PROJECT_VERSION}"*; do
    curl \
        -s \
        -X "POST" \
        -H "Authorization: token ${CODEBERG_API_KEY}" \
        -H "Accept: application/json" \
        -H "Content-Type: multipart/form-data" \
        -F "attachment=@${F}" \
        "https://codeberg.org/api/v1/repos/${ORG}/${PROJECT_NAME}/releases/${RELEASE_ID}/assets"
done
