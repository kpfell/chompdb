require 'buildr/scala'

VERSION_NUMBER = "0.0.1-SNAPSHOT"

repositories.remote << "http://repo1.maven.org/maven2"

F1LESYSTEM = "org.alexboisvert:f1lesystem-core:jar:0.0.2-SNAPSHOT"
PIXII      = "pixii:pixii_2.10.0:jar:0.0.4-SNAPSHOT"
AWS_SDK    = 'com.amazonaws:aws-java-sdk:jar:1.5.5'
AKKA_ACTOR = "com.typesafe.akka:akka-actor_2.10:jar:2.3.0-RC4"

desc "An embeddable distributed BLOB storage library"
define "chompdb" do
  project.version = VERSION_NUMBER
  project.group = "org.alexboisvert"

  desc "Core abstractions and implementation"
  define "core" do
    compile.with F1LESYSTEM, AKKA_ACTOR
    package(:jar)
  end

  desc "Dynamo-based NodeAlive implementation"
  define "dynamodb" do
    compile.with project("core"), PIXII, AWS_SDK
    package(:jar)
  end

  desc "Integration/Smoke testing"
  define "integration" do
    compile.with project("core"), project("core").compile.dependencies
    run.using :main => "chompdb.integration.SmokeTest"
  end
end
