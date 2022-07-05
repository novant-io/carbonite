#! /usr/bin/env fan

using build

class Build : build::BuildPod
{
  new make()
  {
    podName = "carboniteTest"
    summary = "Carbonite Unit Testing"
    version = Version("0.1")
    meta = [
      "license.name": "MIT",
      "vcs.name":     "Git",
      "vcs.uri":      "https://github.com/novant-io/carbonite",
    ]
    depends  = ["sys 1.0", "util 1.0", "concurrent 1.0", "carbonite 0+"]
    srcDirs  = [`fan/`, `test/`]
    docApi = false
    docSrc = false
  }
}
