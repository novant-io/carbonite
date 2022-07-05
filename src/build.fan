#! /usr/bin/env fan

using build

class Build : BuildGroup
{
  new make()
  {
    childrenScripts =
    [
      `carbonite/build.fan`,
      `carboniteTest/build.fan`,
    ]
  }
}
