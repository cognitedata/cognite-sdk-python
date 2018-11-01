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
            image: 'python:3.6.4',
            command: '/bin/cat -',
            resourceRequestCpu: '1000m',
            resourceRequestMemory: '800Mi',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '800Mi',
            envVars: [envVar(key: 'PYTHONPATH', value: '/usr/local/bin')],
            ttyEnabled: true),
    ],
    volumes: [
        secretVolume(secretName: 'jenkins-docker-builder', mountPath: '/jenkins-docker-builder', readOnly: true),
        secretVolume(secretName: 'pypi-credentials', mountPath: '/pypi', readOnly: true),
        configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
    ],
    envVars: [
        secretEnvVar(key: 'COGNITE_TEST_API_KEY', secretName: 'ml-test-api-key', secretKey: 'testkey.txt'),
        secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-token-cognite-sdk-python', secretKey: 'token.txt'),
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
        container('python') {
            stage('Install pipenv') {
                sh("pip3 install pipenv")
            }
            stage('Install dependencies') {
                sh("pipenv sync --dev")
            }
            stage('Test and coverage report') {
                sh("pipenv run pytest --cov-report xml:coverage.xml --cov=cognite --junitxml=test-report.xml || true")
                junit(allowEmptyResults: true, testResults: '**/test-report.xml')
                summarizeTestResults()
            }
            stage('Upload coverage reports') {
                sh 'bash </codecov-script/upload-report.sh'
                step([$class: 'CoberturaPublisher', coberturaReportFile: 'coverage.xml'])
            }
            stage('Build') {
                // sh("pipenv run python3 code_parser.py --remove-type-hints --suppress-warning")
                sh("pipenv run python3 -m black ./cognite -l 120")
                sh("python3 setup.py sdist")
                sh("python3 setup.py bdist_wheel")
            }

            def pipVersion = sh(returnStdout: true, script: 'pipenv run yolk -V cognite-sdk | sort -n | tail -1 | cut -d\\  -f 2').trim()
            def currentVersion = sh(returnStdout: true, script: 'sed -n -e "/^__version__/p" cognite/__init__.py | cut -d\\" -f2').trim()

            println("This version: " + currentVersion)
            println("Latest pip version: " + pipVersion)
            if (env.BRANCH_NAME == 'master' && currentVersion != pipVersion) {
                stage('Release') {
                    sh("pipenv run twine upload --config-file /pypi/.pypirc dist/*")
                }
            }
        }
    }
}
