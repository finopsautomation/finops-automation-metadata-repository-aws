# finops-automation-metadata-repository-aws
FinOps Automation - Metadata Repository AWS using DynamoDB

Component of [finops-automation-metadata-rest-server](https://github.com/finopsautomation/finops-automation-metadata-rest-server).

[![CI/CD](https://github.com/finopsautomation/finops-automation-metadata-repository-aws/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/finopsautomation/finops-automation-metadata-repository-aws/actions/workflows/maven.yml)

## Building from Source

finops-automation-metadata-repository-aws uses a [Apache Maven](https://maven.apache.org/)-based build system.

In the instructions below, `mvnw` is invoked from the root of the source tree and serves as
a cross-platform, self-contained bootstrap mechanism for the build.

### Prerequisites
[Git](https://help.github.com/set-up-git-redirect) and [JDK 17](https://www.oracle.com/technetwork/java/javase/downloads).

Be sure that your `JAVA_HOME` environment variable points to the `jdk-17` folder extracted from the JDK download.

### Check out sources

```
git clone https://github.com/finopsautomation/finops-automation-metadata-repository-aws.git
```

### Compile and run unit tests; build all jars, distribution zips, and docs

Linux
```
./build.sh
```

Windows
```
build
```


