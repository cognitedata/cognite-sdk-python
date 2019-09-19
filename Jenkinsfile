@Library('jenkins-helpers@v0.1.12') _

def label = "cognite-sdk-python-${UUID.randomUUID().toString()}"

podTemplate(
    label: label,
    annotations: [
            podAnnotation(key: "jenkins/build-url", value: env.BUILD_URL ?: ""),
            podAnnotation(key: "jenkins/github-pr-url", value: env.CHANGE_URL ?: ""),
    ],
    containers: [
        containerTemplate(name: 'python',
            image: 'eu.gcr.io/cognitedata/multi-python:7040fac',
            command: '/bin/cat -',
            resourceRequestCpu: '1000m',
            resourceRequestMemory: '800Mi',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '1200Mi',
            ttyEnabled: true),
        containerTemplate(name: 'node',
            image: 'node:slim',
            command: '/bin/cat -',
            resourceRequestCpu: '300m',
            resourceRequestMemory: '300Mi',
            resourceLimitCpu: '300m',
            resourceLimitMemory: '300Mi',
            ttyEnabled: true),
    ],
    volumes: [
        secretVolume(secretName: 'jenkins-docker-builder', mountPath: '/jenkins-docker-builder', readOnly: true),
        secretVolume(secretName: 'pypi-credentials', mountPath: '/pypi', readOnly: true),
        configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
    ],
    envVars: [
        secretEnvVar(key: 'COGNITE_API_KEY', secretName: 'cognite-sdk-python', secretKey: 'integration-test-api-key'),
        secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-tokens', secretKey: 'cognite-sdk-python'),
        envVar(key: 'COGNITE_BASE_URL', value: "https://greenfield.cognitedata.com"),
        envVar(key: 'COGNITE_CLIENT_NAME', value: "python-sdk-integration-tests"),
        envVar(key: 'COGNITE_PROJECT', value: "python-sdk-test"),
        envVar(key: 'CI', value: '1'),
        // /codecov-script/upload-report.sh relies on the following
        // Jenkins and Github environment variables.
        envVar(key: 'BRANCH_NAME', value: env.BRANCH_NAME),
        envVar(key: 'BUILD_NUMBER', value: env.BUILD_NUMBER),
        envVar(key: 'BUILD_URL', value: env.BUILD_URL),
        envVar(key: 'CHANGE_ID', value: env.CHANGE_ID),
    ]) {
    node(label) {
        def gitCommit
        container('jnlp') {
            stage('Checkout') {
                checkout(scm)
                gitCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
            }
        }
        container('node'){
            stage('Download and dereference OpenAPI Spec'){
                sh('npm install -g swagger-cli')
                sh('curl https://storage.googleapis.com/cognitedata-api-docs/dist/v1.json --output spec.json --compressed')
                sh('swagger-cli bundle -r spec.json -o deref-spec.json')
            }
        }
        container('python') {
            stage('Install pipenv') {
                sh("pip3 install pipenv")
            }
            stage('Install core dependencies') {
                sh("pipenv run pip install -r core-requirements.txt")
            }
            stage('Test core') {
                sh("pipenv run pytest tests/tests_unit -m 'not dsl' --test-deps-only-core")
            }
            stage('Install all dependencies') {
                sh("pipenv sync --dev")
            }
            stage('Check code') {
                sh("pipenv run black -l 120 --check .")
            }
            stage('Build Docs'){
                sh("pipenv run pip install .")
                dir('./docs'){
                    sh("pipenv run sphinx-build -W -b html ./source ./build")
                }
            }
            stage('Test OpenAPI Generator'){
                sh('pipenv run pytest openapi/tests')
            }
            stage('Test Client') {
                sh("pyenv local 3.5.0 3.6.6 3.7.2")
                sh("pipenv run tox -p auto")
                junit(allowEmptyResults: true, testResults: '**/test-report.xml')
                summarizeTestResults()
            }
            stage('Upload coverage reports') {
                sh 'bash </codecov-script/upload-report.sh'
                step([$class: 'CoberturaPublisher', coberturaReportFile: 'coverage.xml'])
            }
            stage('Build') {
                sh("python3 setup.py sdist")
                sh("python3 setup.py bdist_wheel")
                sh("python3 setup-core.py sdist")
                sh("python3 setup-core.py bdist_wheel")
            }

            def currentVersion = sh(returnStdout: true, script: 'sed -n -e "/^__version__/p" cognite/client/__init__.py | cut -d\\" -f2').trim()
            println("This version: " + currentVersion)
            def versionExists = sh(returnStdout: true, script: 'pipenv run python3 cognite/client/utils/_version_checker.py -p cognite-sdk -v ' + currentVersion).trim()
            println("Version Exists: " + versionExists)
            if (env.BRANCH_NAME == 'master' && versionExists == 'no') {
                stage('Release') {
                    sh("pipenv run twine upload --config-file /pypi/.pypirc dist/*")
                }
            }
        }
    }
}
