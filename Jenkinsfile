pipeline {
    agent any
    
    environment {
        AWS_REGION = 'us-east-1'
        AWS_ACCOUNT_ID = credentials('aws-account-id')
        ECR_REPOSITORY = 'ingestion-streaming'
        ECS_CLUSTER = 'dev-cluster'
        ECS_SERVICE = 'ingestion-streaming-dev'
        DOCKER_IMAGE = "${ECR_REPOSITORY}:${BUILD_NUMBER}"
    }
    
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }
        
        stage('Build') {
            steps {
                sh 'mvn clean compile'
            }
        }
        
        stage('Unit Tests') {
            steps {
                sh 'mvn test'
            }
            post {
                always {
                    junit 'target/surefire-reports/*.xml'
                }
            }
        }
        
        stage('Code Coverage') {
            steps {
                sh 'mvn jacoco:report'
            }
            post {
                always {
                    // Publish Jacoco coverage report
                    publishHTML([
                        reportDir: 'target/site/jacoco',
                        reportFiles: 'index.html',
                        reportName: 'Jacoco Coverage Report',
                        keepAll: true
                    ])
                    // Also publish for coverage plugins
                    step([
                        $class: 'JacocoPublisher',
                        execPattern: 'target/jacoco.exec',
                        classPattern: 'target/classes',
                        sourcePattern: 'src/main/java'
                    ])
                }
            }
        }
        
        stage('Package') {
            steps {
                sh 'mvn package -DskipTests'
            }
        }
        
        stage('Docker Build') {
            steps {
                script {
                    def dockerImage = docker.build("${DOCKER_IMAGE}", "-f Dockerfile .")
                    // Store image reference for later stages
                    env.DOCKER_IMAGE_ID = dockerImage.id
                }
            }
        }
        
        stage('Docker Push to ECR') {
            steps {
                script {
                    sh """
                        aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
                        docker tag ${DOCKER_IMAGE} ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${BUILD_NUMBER}
                        docker tag ${DOCKER_IMAGE} ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:latest
                        docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${BUILD_NUMBER}
                        docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:latest
                    """
                }
            }
        }
        
        stage('Deploy to AWS Dev') {
            steps {
                script {
                    // Determine deployment target from environment variable
                    def deployTarget = env.DEPLOY_TARGET ?: 'ecs'
                    
                    switch(deployTarget.toLowerCase()) {
                        case 'ecs':
                            echo "Deploying to ECS..."
                            sh """
                                aws ecs update-service \
                                    --cluster ${ECS_CLUSTER} \
                                    --service ${ECS_SERVICE} \
                                    --force-new-deployment \
                                    --region ${AWS_REGION}
                            """
                            break
                        case 'eb':
                            echo "Deploying to Elastic Beanstalk..."
                            sh """
                                aws elasticbeanstalk update-environment \
                                    --environment-name ${env.EB_ENVIRONMENT_NAME ?: 'ingestion-streaming-dev'} \
                                    --version-label ${BUILD_NUMBER} \
                                    --region ${AWS_REGION}
                            """
                            break
                        case 'ec2':
                            echo "Deploying to EC2..."
                            sh """
                                # Update task definition with new image
                                aws ecs register-task-definition \
                                    --cli-input-json file://aws/task-definition.json \
                                    --region ${AWS_REGION}
                                
                                # Force new deployment
                                aws ecs update-service \
                                    --cluster ${ECS_CLUSTER} \
                                    --service ${ECS_SERVICE} \
                                    --force-new-deployment \
                                    --region ${AWS_REGION}
                            """
                            break
                        default:
                            error("Unknown deployment target: ${deployTarget}")
                    }
                }
            }
        }
        
        stage('Health Check') {
            steps {
                script {
                    def healthEndpoint = env.HEALTH_CHECK_URL ?: "http://your-dev-alb-endpoint/actuator/health"
                    def maxAttempts = 30
                    def waitTime = 10
                    def healthy = false
                    
                    echo "Waiting for service to become healthy at ${healthEndpoint}..."
                    
                    for (int i = 0; i < maxAttempts; i++) {
                        try {
                            def response = sh(
                                script: "curl -f -s ${healthEndpoint} || exit 1",
                                returnStatus: true
                            )
                            if (response == 0) {
                                healthy = true
                                echo "Health check passed on attempt ${i+1}"
                                break
                            }
                        } catch (Exception e) {
                            echo "Health check attempt ${i+1}/${maxAttempts} failed, retrying in ${waitTime}s..."
                        }
                        sleep(waitTime)
                    }
                    
                    if (!healthy) {
                        error("Health check failed after ${maxAttempts} attempts. Service may not be healthy.")
                    } else {
                        echo "✅ Service is healthy!"
                    }
                }
            }
        }
    }
    
    post {
        success {
            echo 'Pipeline succeeded!'
        }
        failure {
            echo 'Pipeline failed!'
        }
        always {
            cleanWs()
        }
    }
}

