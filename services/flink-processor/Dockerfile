FROM flink:1.18-scala_2.12-java11

# apache-flink 1.18 supports Python 3.7-3.10.
# Ubuntu Jammy ships Python 3.10 as default, which has a pre-built
# numpy==1.21.4 arm64 wheel — exactly what apache-flink's build needs.
#
# PyFlink's setup.py also validates JDK headers at $JAVA_HOME/include,
# but the Flink image bundles Eclipse Temurin JRE (no headers).
# Install openjdk-11-jdk from apt for the headers and symlink them into
# the Temurin path so Flink's own JVM is still used at runtime.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-dev python3-pip \
        openjdk-11-jdk build-essential && \
    JNI_INCLUDE=$(dirname "$(find /usr/lib/jvm -name 'jni.h' 2>/dev/null | head -1)") && \
    ln -sf "$JNI_INCLUDE" /opt/java/openjdk/include && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.18.1

WORKDIR /opt/flink/jobs
