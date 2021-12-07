// chfs client.  implements FS operations using extent server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <list>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum chfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string chfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool chfs_client::isfile(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is not a file\n", inum);
    return false;
}

/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool chfs_client::isdir(inum inum) {
    // Oops! is this still correct when you implement symlink?
//    return !isfile(inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    }
    printf("isfile: %lld is not a dir\n", inum);
    return false;

}

int chfs_client::getfile(inum inum, fileinfo &fin) {
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

    release:
    return r;
}

int chfs_client::getdir(inum inum, dirinfo &din) {
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int chfs_client::setattr(inum ino, size_t size) {
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;
    if (ec->get(ino, buf) != OK) {
        return IOERR;
    }
    buf.resize(size);
    if (ec->put(ino, buf) != OK) {
        return IOERR;
    }
    return r;
}

int chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent information.
     */
    bool found = false;
    inum node;
    int status = lookup(parent, name, found, node);
    if (status != OK && status != EXIST) {
        return IOERR;
    }
    if (found) {
        return EXIST;
    }

    std::string buf;
    ec->create(extent_protocol::T_FILE, ino_out);
//    std::cout<<"chfs_client: ec->get parent="<<parent<<std::endl;
    ec->get(parent, buf);
//    buf = buf + "<" + name + "," + filename(ino_out) + ">";
    buf = buf + name + "`" + filename(ino_out) + "^";     // <name, inode>
//    std::cout<<"hello, created file called , length="<<buf.length()<<" "<<buf<<std::endl;
//    std::cout<<"chfs_client: ec->put parent="<<parent<<std::endl;
    ec->put(parent, buf);
    return r;
}

int chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    inum node;
    int status = lookup(parent, name, found, node);
    if (status != OK && status != EXIST) {
        return IOERR;
    }
    if (found) {
        return EXIST;
    }

    std::string buf;
    ec->create(extent_protocol::T_DIR, ino_out);
    ec->get(parent, buf);
    buf = buf + std::string(name) + "`" + filename(ino_out) + "^";     // <name, inode>
    ec->put(parent, buf);
    return r;
}

int chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    extent_protocol::attr a;
    ec->getattr(parent, a);
    if (a.type != extent_protocol::T_DIR) {
        return IOERR;
    }

    std::list <dirent> dirents;
    readdir(parent, dirents);
    std::list<dirent>::iterator iter;
    found = false;
    ino_out = 0;
    for (iter = dirents.begin(); iter != dirents.end(); iter++) {
        if (std::string(name) == iter->name) {
            found = true;
            ino_out = iter->inum;
            break;
        }
    }
    return r;
}

int chfs_client::readdir(inum dir, std::list <dirent> &list) {
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    extent_protocol::attr a;
    ec->getattr(dir, a);
    if (a.type != extent_protocol::T_DIR) {
        return IOERR;
    }

    std::string buf;
    ec->get(dir, buf);
    int len = buf.length();
    int now = 0;
    dirent now_dirent;
    list.clear();
    while (now < len) {
        size_t end = buf.find('^', now);
        size_t div = buf.find('`', now);
        if (end == std::string::npos || div == std::string::npos) break;
        now_dirent.name = buf.substr(now, div - now);  //now ~ div-1
        now_dirent.inum = n2i(buf.substr(div + 1, end - div - 1));  // div+1 ~ end-1
        list.push_back(now_dirent);
        now = end + 1;
    }
    return r;
}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data) {
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    extent_protocol::status ret = ec->get(ino, buf);
    if (ret != extent_protocol::OK) {
        return IOERR;
    }
    if ((uint32_t) off >= buf.size()) {
        //     std::cout << "chfs_client::read: @off is greater than the size of file, read zero bytes." << std::endl;
        return r;
    }
    data = buf.substr(off, size);
    //std::cout << "chfs_client::read:" << ino << " " << size << " " << off << " " << data << std::endl;
    return r;
}

int chfs_client::write(inum ino, size_t size, off_t off, const char *data,
                       size_t &bytes_written) {
    int r = OK;
    //  std::cout << "chfs_client::write:" << ino << " " << size << " " << off << " " << data << std::endl;
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    extent_protocol::status ret = ec->get(ino, buf);
    if (ret != extent_protocol::OK) {
        return IOERR;
    }
    if (off + size > buf.size()) {
        buf.resize(off + size);
    }
//    buf.replace(off, size, data);
    for (uint32_t i = off; i < off + size; i++) {
        buf[i] = data[i - off];
    }
    bytes_written = size;

    ret = ec->put(ino, buf);
    if (ret != extent_protocol::OK) {
        return IOERR;
    }
    return r;
}

int chfs_client::unlink(inum parent, const char *name) {
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found = false;
    inum node;
    if (lookup(parent, name, found, node) != OK) {
        return IOERR;
    }
    if (!found) {
        return r;
    }
    ec->remove(node);

    std::string buf;
    if (ec->get(parent, buf) != OK) {
        return IOERR;
    }
    int len = buf.length();
    size_t from = buf.find(name);
    if (from == std::string::npos) {
        return r;
    }
    size_t to = buf.find('^', from);
    if (to == std::string::npos) {
        buf.erase(from, len - from + 1);
    } else {
        buf.erase(from, to - from + 1);
    }
    ec->put(parent, buf);
    return r;
}


int chfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out) {
    int r = OK;
    bool found = false;
    inum node;
    std::string buf;
    if (lookup(parent, name, found, node) != OK) {
        return IOERR;
    }
    if (found) {
        return EXIST;
    }

    ec->get(parent, buf);
    ec->create(extent_protocol::T_SYMLINK, ino_out);
    ec->put(ino_out, std::string(link));
    buf = buf + name + "`" + filename(ino_out) + "^";     // <name, inode>
    ec->put(parent, buf);
    return r;
}

int chfs_client::readlink(inum ino, std::string &buf) {
    int r = OK;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        return IOERR;
    }
    return r;
}
