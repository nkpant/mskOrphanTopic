#!/bin/bash

# Define log file
LOG_FILE="setup_msk_environment.log"

log() {
    echo "$1" | tee -a $LOG_FILE
}

# Ask if user wants to use a specified profile
echo "Do you want to use a specific AWS user profile? The profile is used to find list of active MSK connectors (yes/no)"
read use_profile

if [ "$use_profile" == "yes" ]; then
  echo "Please input your AWS profile name:"
  read aws_profile
  profile_param="--profile $aws_profile"
else
  echo "No profile specified, using default profile/IAM role."
  profile_param=""
fi

# Check if AWS CLI v2 is installed ..this is to be removed later on ..also check on different cli versions
if command -v aws &> /dev/null && aws --version | grep -q "aws-cli/2"; then
    log "AWS CLI v2 is already installed."
else
    read -p "AWS CLI v2 is not installed. Do you want to install it? (yes/no): " install_aws
    if [[ $install_aws == "yes" ]]; then
        log "User agreed to install AWS CLI v2."
        # Assuming that the OS is Linux, installation via package manager
        curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
        unzip awscliv2.zip
        sudo ./aws/install
        rm -rf awscliv2.zip aws/
        log "AWS CLI v2 installed."
    else
        log "User did not agree to install AWS CLI v2. Exiting."
        exit 1
    fi
fi

# Check if MSK library is installed ...hard code to 3.8.0 for now ..later check if it can automatically pick up latest version
KAFKA_HOME="./kafka_2.12-3.8.0"
if [ -d "$KAFKA_HOME" ]; then
    log "MSK library is already installed."
else
    read -p "MSK library is not installed. Do you want to install it? (yes/no): " install_msk
    if [[ $install_msk == "yes" ]]; then
        log "User agreed to install MSK libraries."
        wget https://archive.apache.org/dist/kafka/3.8.0/kafka_2.12-3.8.0.tgz
        tar xzf kafka_2.12-3.8.0.tgz
        rm kafka_2.12-3.8.0.tgz
        log "MSK libraries installed."
    else
        log "User did not agree to install MSK libraries. Exiting."
        exit 1
    fi
fi

# Check if telnet is installed.. phase 1 - check with telnet ...phase 2 - introduce and test curl and/or netcat ..yum install is speciic to RHEL/aws linux for now ...later on add apt options in addition to yum install
if command -v telnet &> /dev/null; then
    log "Telnet is already installed."
else
    read -p "Telnet is not installed. It helps in testing network connectivity to the bootstrap server. Do you want to install it? (yes/no): " install_telnet
    if [[ $install_telnet == "yes" ]]; then
        log "User agreed to install telnet."
        sudo yum install telnet -y
    else
        log "User did not agree to install telnet. Skipping connectivity tests."
    fi
fi

# Check if Java is installed ...later check if there is java executible in a specific location
if command -v java &> /dev/null; then
    log "Java is already installed."
else
    read -p "Java is not installed, and it is required. Do you want to install it? (yes/no): " install_java
    if [[ $install_java == "yes" ]]; then
        log "User agreed to install Java."
        sudo yum install java -y
    else
        log "User did not agree to install Java. Exiting."
        exit 1
    fi
fi

# Ask for MSK authentication method ..niraj comment 08292024 - later add use case for mTLS also
read -p "Which MSK authentication method do you use? (IAM/SASL-SCRAM): " auth_method
if [[ $auth_method == "SASL-SCRAM" ]]; then
    # Step 6.1: Gather SASL-SCRAM user inputs
    read -p "Enter username: " username
    read -sp "Enter password: " password
    echo
    read -p "Enter JKS certificate location: " jks_location

    cat << EOF > "$KAFKA_HOME/bin/client.properties"
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="$password";
ssl.truststore.location=$jks_location
EOF
    log "Client properties for SASL-SCRAM created."
else
    # Check if MSK IAM JAR is available
    MSK_IAM_JAR="$KAFKA_HOME/bin/aws-msk-iam-auth-2.2.0-all.jar"
    if [ -f "$MSK_IAM_JAR" ]; then
        log "MSK IAM JAR file is already available."
    else
        read -p "MSK IAM JAR file is required but not found. Do you want to download it? (yes/no): " download_jar
        if [[ $download_jar == "yes" ]]; then
            wget https://github.com/aws/aws-msk-iam-auth/releases/download/v2.2.0/aws-msk-iam-auth-2.2.0-all.jar -P "$KAFKA_HOME/bin/"
            log "MSK IAM JAR file downloaded."
        else
            log "User did not agree to download MSK IAM JAR file. Exiting."
            exit 1
        fi
    fi

    # Create client.properties for IAM
    cat << EOF > "$KAFKA_HOME/bin/client.properties"
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF
    log "Client properties for IAM created."
fi

# Ask user for MSK bootstrap servers
read -p "Enter comma-separated list of AWS MSK bootstrap servers including port numbers: " bootstrap_servers
IFS=',' read -ra ADDR <<< "$bootstrap_servers"
reversed_servers=$(echo "${ADDR[@]}" | tac -s ' ' | tr ' ' ',')

# Validate server format that it has <url name>:<port> format. run against niraj_local_use_case.sh to verify different test cases
for server in ${reversed_servers//,/ }; do
    if ! [[ "$server" =~ ^(boot-.*kafka-serverless\.[a-z0-9-]+\.amazonaws\.com:9098|b-[1-3]\..*\.kafka\.[a-z0-9-]+\.amazonaws\.com:[0-9]+)$ ]]; then
        log "Invalid server format detected. Please re-enter the list of bootstrap servers."
        read -p "Enter comma-separated list of AWS MSK bootstrap servers including port numbers: " bootstrap_servers
        IFS=',' read -ra ADDR <<< "$bootstrap_servers"
        reversed_servers=$(echo "${ADDR[@]}" | tac -s ' ' | tr ' ' ',')
    fi
done

# Verify acces to each bootstrap server
for server in ${reversed_servers//,/ }; do
    log "Processing server: $server"

    if [[ $install_telnet == "yes" ]]; then
        # Step 10: Test connectivity using telnet
        server_name=$(echo "$server" | cut -d':' -f1)
        port=$(echo "$server" | cut -d':' -f2)
        if timeout 10 telnet $server_name $port | grep -q "Connected to"; then
            log "Connectivity to $server verified."
        else
            log "Failed to connect to $server."
        fi
    fi
done

# Set environment variables
export CLASSPATH="$KAFKA_HOME/bin/aws-msk-iam-auth-2.2.0-all.jar"
export KAFKA_HEAP_OPTS="-Xms4096M -Xmx4096M"
log "Environment variables set for MSK."


# List active connectors
active_connectors=$(aws kafkaconnect list-connectors --cli-connect-timeout 20 $profile_param | jq '.connectors[] | select(.connectorState == "CREATING" or .connectorState == "RUNNING") | .connectorName')
if [ -z "$active_connectors" ]; then
    log "No active connectors found or AWS CLI command timed out."
    exit 0
else
    log "Active connectors identified: $active_connectors"
fi

# List topics
topics=$(kafka_2.12-3.8.0/bin/kafka-topics.sh --bootstrap-server $reversed_servers --command-config kafka_2.12-3.8.0/bin/client.properties --list)
if [[ $? -ne 0 ]]; then
    log "kafka-topics.sh command timed out after 60 seconds."
    exit 1
fi

# Split the topics into an array, removing quotes
IFS=' ' read -r -a topics_array <<< "$(echo $topics | tr -d '"')"

# First strip all beginning and ending double quotes
active_connectors=$(echo "$active_connectors" | sed 's/"//g')

# Split the active connectors into an array
IFS=' ' read -r -a active_connectors_array <<< "$active_connectors"

# Array to store filtered topics
orphan_topics=()

# Loop through the topics and check if they meet the conditions
for topic in "${topics_array[@]}"; do
  # Check if the topic starts with the specified prefixes
  if [[ $topic == __amazon_msk_connect_configs_* || $topic == __amazon_msk_connect_offsets_* || $topic == __amazon_msk_connect_status_* ]]; then
    # Flag to indicate if the topic matches any active connector
    match_found=false
    temptopic=$(echo $topic | sed -e 's/__amazon_msk_connect_[a-z]*_//')
    # Loop through the active connectors and check for pattern matches
    for connector in "${active_connectors_array[@]}"; do
           temptopic="${temptopic%_*}" 
	    if [[ $temptopic == *$connector* ]]; then
	      match_found=true
        break
      fi
    done

    # If no match is found, add the topic to the orphan topic list
    if [ "$match_found" = false ]; then
      orphan_topics+=("$topic")
    fi
  fi
done

# niraj - Print the orphan topics for debug only
#if [ ${#orphan_topics[@]} -eq 0 ]; then
#  echo "No topics match the criteria."
#else
#  echo "Orphan topics:"
#  for topic in "${orphan_topics[@]}"; do
#    echo "$topic"
#  done
#fi

# Generate command to delete orphan topics..maybe 
echo "# Set the following environment variables to set minimum memory required to delete the topic" > delete_topics.sh
echo "export KAFKA_HEAP_OPTS=\"-Xms4096M -Xmx4096M\"" >> delete_topics.sh

for topic in "${orphan_topics[@]}"; do
    echo "./kafka_2.12-3.8.0/bin/kafka-topics.sh --bootstrap-server $reverse_bootstrap_servers --delete --topic $topic --command-config kafka_2.12-3.8.0/bin/client.properties" >> ./delete_topics.sh
done

chmod 755 delete_topics.sh

# Log all decision-making steps
log "Script completed successfully."