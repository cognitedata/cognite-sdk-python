# exit when any command fails
set -e

packageVersion=$(sed -n -e "/^__version__/p" cognite/client/__init__.py | cut -d\" -f2)
branchName="bot/pythonCodeSnippets_v$packageVersion"
message="[Python SDK]: update code snippets to v$packageVersion"
snippets_filename="python-sdk-examples.json"
snippets_path="./services/service_contracts/versions/v1/$snippets_filename"
request_body="{\"title\": \"$message\", \"head\": \"$branchName\", \"base\": \"master\"}"
request_header="Authorization: token ${GH_TOKEN}"
github_url="https://api.github.com/repos/cognitedata/infrastructure/pulls"

git config --global user.email "cognite-cicd@users.noreply.github.com"
git config --global user.name "Cognite CICD"
git clone --depth 1 https://$GH_TOKEN@github.com/cognitedata/infrastructure.git >/dev/null 2>&1
cd infrastructure

git checkout -b "$branchName"
cp "../$snippets_filename" "$snippets_path"
git add "$snippets_path"
git commit --author="$author" -m "$message"
git push origin "$branchName"
curl -H "$request_header" -X POST -d "$request_body" "$github_url"

cd ../
rm -rf infrastructure
rm "$snippets_filename"
