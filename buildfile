require 'buildr/scala'

VERSION_NUMBER = "0.0.1-SNAPSHOT"

repositories.remote << "http://repo1.maven.org/maven2"

f1lesystem = "org.alexboisvert:f1lesystem-core:jar:0.0.1-SNAPSHOT"
pixii = "pixii:pixii_2.10.0:jar:0.0.4-SNAPSHOT"
AWS_SDK = 'com.amazonaws:aws-java-sdk:jar:1.5.5'

desc "An embeddable distributed BLOB storage library"
define "chompdb" do
  project.version = VERSION_NUMBER
  project.group = "org.alexboisvert"

  compile.with f1lesystem, pixii, AWS_SDK
  package(:jar)
end
