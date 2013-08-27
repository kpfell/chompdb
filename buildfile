require 'buildr/scala'

VERSION_NUMBER = "0.0.1-SNAPSHOT"

repositories.remote << "http://repo1.maven.org/maven2"

f1lesystem = "org.alexboisvert:f1lesystem-core:jar:0.0.1-SNAPSHOT"

desc "An embeddable distributed BLOB storage library"
define "chompdb" do
  project.version = VERSION_NUMBER
  project.group = "org.alexboisvert"

  compile.with f1lesystem
  package(:jar)
end
