package com.samlaberge

/**
 * Class loader which uses a provided file lookup stub to lookup any classes which cannot
 * be found locally.
 * @param parent parent class loader, will consult them first to see if we need to go to the network
 * @param fileService stub used to lookup class files.
 */
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
