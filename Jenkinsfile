@Library('jenkins-helpers@v0.1.8') _

podTemplate(
    label: 'jnlp-cognite-sdk-python',
    containers: [
        containerTemplate(name: 'python',
            image: 'python:3.6.4',
            command: '/bin/cat -',
            resourceRequestCpu: '1000m',
            resourceRequestMemory: '500Mi',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '500Mi',
            envVars: [envVar(key: 'PYTHONPATH', value: '/usr/local/bin')],
            ttyEnabled: true),
    ],
    volumes: [
        secretVolume(secretName: 'jenkins-docker-builder', mountPath: '/jenkins-docker-builder', readOnly: true),
        secretVolume(secretName: 'pypi-credentials', mountPath: '/pypi', readOnly: true),
        configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
    ],
    envVars: [
        secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-token-cognite-sdk-python', secretKey: 'token.txt'),
        // /codecov-script/upload-report.sh relies on the following
        // Jenkins and Github environment variables.
        envVar(key: 'BRANCH_NAME', value: env.BRANCH_NAME),
        envVar(key: 'BUILD_NUMBER', value: env.BUILD_NUMBER),
        envVar(key: 'BUILD_URL', value: env.BUILD_URL),
        envVar(key: 'CHANGE_ID', value: env.CHANGE_ID),
    ]) {
    node('jnlp-cognite-sdk-python') {
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
                sh("pipenv install -d")
                sh("pip3 install .")
            }
            stage('Test and coverage report') {
                sh("pipenv run coverage run --source cognite unit_tests/run_tests.py")
                sh("pipenv run coverage xml")
            }
            stage('Upload coverage reports') {
                sh 'bash </codecov-script/upload-report.sh'
                step([$class: 'CoberturaPublisher', coberturaReportFile: 'coverage.xml'])
            }
            stage('Build') {
                sh("python3 setup.py sdist")
                sh("python3 setup.py bdist_wheel")
            }

            def pipVersion = sh(returnStdout: true, script: 'pipenv run yolk -V cognite-sdk | sort -n | tail -1 | cut -d\\  -f 2').trim()
            def currentVersion = sh(returnStdout: true, script: 'pipenv run python3 -c "import cognite; print(cognite.__version__)"').trim()

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
