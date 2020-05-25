# exit when any command fails
set -e

packageVersion=$(sed -n -e "/^__version__/p" cognite/client/__init__.py | cut -d\" -f2)
branchName="bot/pythonCodeSnippets_v$packageVersion"
message="[Python SDK]: update code snippets to v$packageVersion"
snippets_filename="python-sdk-examples.json"
snippets_path="./versions/v1/$snippets_filename"
request_body="{\"title\": \"$message\", \"head\": \"$branchName\", \"base\": \"master\"}"
request_header="Authorization: token ${GH_TOKEN}"
github_url="https://api.github.com/repos/cognitedata/service-contracts/pulls"

git config --global user.email "cognite-cicd@users.noreply.github.com"
git config --global user.name "Cognite CICD"
git clone https://$GH_TOKEN@github.com/cognitedata/service-contracts.git >/dev/null 2>&1
cd service-contracts

git checkout -b "$branchName"
cp "../$snippets_filename" "$snippets_path"
git add "$snippets_path"
git commit --author="$author" -m "$message"
git push origin "$branchName"
curl -H "$request_header" -X POST -d "$request_body" "$github_url"

cd ../
rm -rf service-contracts
rm "$snippets_filename"
