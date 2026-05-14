pipeline {
    agent any

    environment {
        REGISTORY_URL = "https://asia-southeast1-docker.pkg.dev"
        IMAGE_NAME = "<YOUR_REGISTRY>/<YOUR_PROJECT>/prefect_v3/ingest/ingest_gcp_postgresql_ecoapp   // [SCRUBBED] replace with your Docker registry path"
        DOCKER_CREDENTIALS_ID = 'artifacts-registory-file'
        NAME_REPO = 'ingest_gcp_postgresql_ecoapp'
        TAG = setGitBranchName()
        BRANCH_NAME = getGitBranchName()

        // Prefect v3
        PREFECT_API_URL = "http://<PREFECT_SERVER_IP>:4200/api   // [SCRUBBED] replace with your Prefect server IP/hostname"
        PREFECT_WORK_POOL = "docker-pool"
        PREFECT_WORK_QUEUE = "default"
    }

    tools {
        maven 'Maven_3_6_3'
    }

    stages {

        stage('Who am i') {
            steps {
                sh 'whoami'
            }
        }

        stage('Check workspace') {
            steps {
                sh 'echo $JOB_NAME'
                sh 'echo $WORKSPACE'
                sh 'echo $BRANCH_NAME'
                sh 'echo ${IMAGE_NAME}:${TAG}'
            }
        }

        stage('Code Analysis with SonarQube') {
            when {
                anyOf { branch 'develop'; branch 'main'; tag "*" }
            }
            steps {
                withCredentials([string(credentialsId: 'SONAR_TOKEN', variable: 'SONAR_TOKEN')]) {
                    sh """
                        mvn clean verify sonar:sonar \
                          -Dsonar.host.url=${SONAR_HOST_URL} \
                          -Dsonar.projectKey=${NAME_REPO} \
                          -Dsonar.branch.target=${BRANCH_NAME} \
                          -Dsonar.login=${SONAR_TOKEN}
                    """
                }
            }
        }

        stage('Validate branch') {
            steps {
                script {
                    echo "Start pipeline from: ${BRANCH_NAME}"
                    if (!(BRANCH_NAME in ['main', 'develop', 'production'] || env.GIT_TAG)) {
                        currentBuild.result = 'NOT_BUILT'
                        error("Skip pipeline for branch: ${BRANCH_NAME}")
                    }
                }
            }
        }

        stage('Print image tag') {
            when {
                anyOf { branch 'develop'; branch 'main' }
            }
            steps {
                sh 'echo ${IMAGE_NAME}:${TAG}'
            }
        }

        stage('Build image') {
            when {
                anyOf { branch 'develop'; branch 'main' }
            }
            steps {
                withCredentials([
                    file(credentialsId: DOCKER_CREDENTIALS_ID, variable: 'GCR_KEY'),
                    string(credentialsId: 'NPM_TOKEN', variable: 'NPM_TOKEN')
                ]) {
                    sh """
                        cat $GCR_KEY | docker login -u _json_key --password-stdin $REGISTORY_URL
                        docker build \
                          --build-arg NPM_TOKEN=${NPM_TOKEN} \
                          --build-arg IMAGE_TAG=${IMAGE_NAME}:${TAG} \
                          -t ${IMAGE_NAME}:${TAG} .
                    """
                }
            }
        }

        stage('Push image') {
            when {
                anyOf { branch 'develop'; branch 'main' }
            }
            steps {
                withCredentials([file(credentialsId: DOCKER_CREDENTIALS_ID, variable: 'GCR_KEY')]) {
                    sh """
                        cat $GCR_KEY | docker login -u _json_key --password-stdin $REGISTORY_URL
                        docker push ${IMAGE_NAME}:${TAG}
                    """
                }
            }
        }

        stage('Deploy Prefect v3') {
            when {
                anyOf { branch 'develop'; branch 'main' }
            }
            steps {
                withCredentials([file(credentialsId: DOCKER_CREDENTIALS_ID, variable: 'GCR_KEY')]) {
                    sh """
                        docker run --rm --network host \
                          -v ${GCR_KEY}:/tmp/gcp-sa-key.json:ro \
                          -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-sa-key.json \
                          -e PREFECT_DEPLOY_MODE=1 \
                          -e PREFECT_API_URL=${PREFECT_API_URL} \
                          -e CI_COMMIT_BRANCH=${BRANCH_NAME} \
                          -e IMAGE_TAG=${IMAGE_NAME}:${TAG} \
                          -e PREFECT_WORK_POOL=${PREFECT_WORK_POOL} \
                          -e PREFECT_WORK_QUEUE=${PREFECT_WORK_QUEUE} \
                          ${IMAGE_NAME}:${TAG} \
                          bash -c '
                            prefect version &&
                            prefect config view &&
                            prefect deploy --all
                          '
                    """
                }
            }
        }

        stage('Remove local image') {
            when {
                anyOf { branch 'develop'; branch 'main' }
            }
            steps {
                sh "docker rmi -f ${IMAGE_NAME}:${TAG} || true"
            }
        }
    }
}

def getGitBranchName() {
    def branch_name = GIT_BRANCH
    if (branch_name.contains("origin/")) {
        branch_name = branch_name.split("origin/")[1]
    }
    return branch_name
}

def setGitBranchName() {
    def branch_name = GIT_BRANCH
    def commitSha = GIT_COMMIT.take(8)
    if (branch_name.contains("origin/")) {
        branch_name = branch_name.split("origin/")[1]
    }
    return "${branch_name}-${commitSha}"
}