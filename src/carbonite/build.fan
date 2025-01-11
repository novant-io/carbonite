#! /usr/bin/env fan

using build

class Build : build::BuildPod
{
  new make()
  {
    podName = "carbonite"
    summary = "Carbonite ORM for SQL databases"
    version = Version("0.1")
    meta = [
      "license.name": "MIT",
      "vcs.name":     "Git",
      "vcs.uri":      "https://github.com/novant-io/carbonite",
      "repo.public":  "true",
      "repo.tags":    "database",
    ]
    depends  = ["sys 1.0", "util 1.0", "concurrent 1.0"]
    srcDirs  = [`fan/`, `fan/impl/`, `fan/sql/`, `test/`]
    javaDirs = [`java/`]
    // resDirs = [`doc/`]
    docApi = true
    docSrc = true
  }
}
