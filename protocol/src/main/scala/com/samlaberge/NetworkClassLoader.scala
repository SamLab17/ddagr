package com.samlaberge

class NetworkClassLoader(parent: ClassLoader, fileService: FileLookupStub) extends ClassLoader(parent) {

  override def findClass(name: String): Class[_] = {
    fileService.lookupClass(name) match {
      case Some(bytes) => defineClass(name, bytes, 0, bytes.length)
      case None => throw new ClassNotFoundException(s"Could not find class $name")
    }
  }

  override def loadClass(name: String): Class[_] = {
    var loadedClass = findLoadedClass(name)
    if(loadedClass != null) {
      return loadedClass
    }
    loadedClass = super.loadClass(name)
    if(loadedClass != null) {
      return loadedClass
    }

    try {
      loadedClass = findClass(name)
    } catch {
      case _: ClassNotFoundException => loadedClass = null
      case _: Throwable => loadedClass = null
    }
    loadedClass
  }
}
