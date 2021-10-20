#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk() {
    bzero(blocks, sizeof(blocks));
}


void disk::read_block(blockid_t id, char *buf) {
    if (id < 0 || id >= BLOCK_NUM) {
        return;
    }
    //memcpy(void *destin, void *source, unsigned n)
    memcpy(buf, (char *) blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf) {
    if (id < 0 || id >= BLOCK_NUM) {
        return;
    }
//    std::cout<<"disk:write_block id="<<id<<" size="<<strlen(buf)<<std::endl;
//    std::cout<<buf<<std::endl;
    memcpy((char *) blocks[id], buf, BLOCK_SIZE);
//    std::cout<<"disk:write_block completed id="<<id<<std::endl;
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block() {
    /*
     * your code goes here.
     * note: you should mark the corresponding bit in block bitmap when alloc.
     * you need to think about which block you can start to be allocated.
     */
    //allocate after inode, find one by one
    uint32_t from = IBLOCK(sb.ninodes, sb.nblocks) + 1;
    for (uint32_t i = from; i < BLOCK_NUM; i++) {
        if (using_blocks[i] != 1) {
            using_blocks[i] = 1;
            return i;
        }
    }
    return 0;
}

void block_manager::free_block(uint32_t id) {
    /*
     * your code goes here.
     * note: you should unmark the corresponding bit in the block bitmap when free.
     */
    if (id < 0 || id >= BLOCK_NUM) {
        return;
    }
    using_blocks[id] = 0;

    return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager() {
    d = new disk();

    // format the disk
    sb.size = BLOCK_SIZE * BLOCK_NUM;
    sb.nblocks = BLOCK_NUM;
    sb.ninodes = INODE_NUM;

}

void block_manager::read_block(uint32_t id, char *buf) {
    d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf) {
    d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager() {
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
    if (root_dir != 1) {
        printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

/* Create a new file.
 * Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type) {
    /*
     * your code goes here.
     * note: the normal inode block should begin from the 2nd inode block.
     * the 1st is used for root_dir, see inode_manager::inode_manager().
     */
    inode *node;
    node = new inode();
    node->type = type;
    node->size = 0;
    node->atime = node->ctime = node->mtime = time(0);
    memset(node->blocks, 0, sizeof(node->blocks));
    uint32_t from = 1;
    if (type == extent_protocol::T_DIR) {
        from = 1;
    } else {
        from = 2;
    }
    for (uint32_t i = from; i < INODE_NUM; i++) {
        if (get_inode(i) == NULL) {
            put_inode(i, node);
            delete node;
            node = nullptr;
            return i;
        }
    }
    return 1;
}

void inode_manager::free_inode(uint32_t inum) {
    /*
     * your code goes here.
     * note: you need to check if the inode is already a freed one;
     * if not, clear it, and remember to write back to disk.
     */
    struct inode *tmp = get_inode(inum);
    if (tmp == NULL) {
        delete tmp;
        return;
    }
    struct inode *node;
    node = new inode();
    node->type = 0;
    node->size = 0;
    node->atime = node->ctime = node->mtime = time(0);
    memset(node->blocks, 0, sizeof(node->blocks));
    put_inode(inum, node);
    delete node;
    node = nullptr;
    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *inode_manager::get_inode(uint32_t inum) {
    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];

    printf("\tim: get_inode %d\n", inum);

    if (inum < 0 || inum >= INODE_NUM) {
        printf("\tim: inum out of range\n");
        return NULL;
    }

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    // printf("%s:%d\n", __FILE__, __LINE__);

    ino_disk = (struct inode *) buf + inum % IPB;
    if (ino_disk->type == 0) {
        printf("\tim: inode not exist\n");
        return NULL;
    }

    ino = (struct inode *) malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino) {
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;

    printf("\tim: put_inode %d\n", inum);
    if (ino == NULL)
        return;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode *) buf + inum % IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

uint32_t inode_manager::read_bytes(char *buf) {
//    uint32_t index[BLOCK_SIZE];
//    bm->read_block(node->blocks[NDIRECT], buf);
//    memcpy(index, buf, BLOCK_SIZE);
    uint32_t id;
    for (int i = 0; i < 4; i++) {
        *((char *) &id + i) = *(buf + i);
    }
    return id;
}

void inode_manager::write_bytes(char *buf, uint32_t id) {
    for (int i = 0; i < 4; i++) {
        *(buf + i) = *((char *) &id + i);
    }
}

#define MIN(a, b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return allocated data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size) {
    /*
     * your code goes here.
     * note: read blocks related to inode number inum,
     * and copy them to buf_out
     */
    inode *node = get_inode(inum);
    if (node == nullptr) return;
    //读取block修改atime
    node->atime = time(0);
    *size = node->size;

    uint32_t direct = *size <= NDIRECT * BLOCK_SIZE ? *size : (NDIRECT * BLOCK_SIZE);
    uint32_t direct_size =direct % BLOCK_SIZE ? (direct / BLOCK_SIZE + 1): (direct / BLOCK_SIZE);
    uint32_t blocks = *size % BLOCK_SIZE ? *size / BLOCK_SIZE + 1 : *size / BLOCK_SIZE;
    *buf_out = (char *) malloc(*size);

    uint32_t rest = direct,i;
    char block[BLOCK_SIZE];
    //先从direct里读
    for (i = 0; i < direct_size; ++i) {
        uint32_t block_id = node->blocks[i];
        uint32_t written = rest > BLOCK_SIZE ? BLOCK_SIZE : rest;
        bm->read_block(block_id, block);
        memcpy(*buf_out + i * BLOCK_SIZE, block, written);
        rest -= written;
    }
    //还没读完 到indirect里读
    if (blocks > direct_size) {
        uint32_t id = node->blocks[NDIRECT];
        char tmp[BLOCK_SIZE];
        bm->read_block(id, tmp);

        uint32_t indirect = *size - direct;
        rest = indirect;

        for (; i < blocks; ++i) {
            uint32_t written = rest > BLOCK_SIZE ? BLOCK_SIZE : rest;
            uint32_t block_id = read_bytes(&tmp[4 * (i - direct_size)]);
            bm->read_block(block_id, block);
            memcpy(*buf_out + i * BLOCK_SIZE, block, written);
            rest -= written;
        }
    }
    put_inode(inum, node);
    delete node;
    node = nullptr;
    return;
}

void inode_manager::write_file(uint32_t inum, const char *buf, int size) {
    /*
     * your code goes here.
     * note: write buf to blocks of inode inum.
     * you need to consider the situation when the size of buf
     * is larger or smaller than the size of original inode
     */
//    std::cout<<"inode_manager::write_file called inum="<<inum<<" size="<<size<<std::endl;
    if (size < 0 || (uint32_t) size > BLOCK_SIZE * MAXFILE) return;
    inode *node = get_inode(inum);
    if (node == NULL) return;
    //写文件时修改ctime,mtime
    node->ctime = time(0);
    node->mtime = time(0);
    node->size = size;

    uint32_t direct = size <= NDIRECT * BLOCK_SIZE ? size : (NDIRECT * BLOCK_SIZE);
    //原来-1除有问题 此处进行修改 防止溢出
    uint32_t direct_size = direct % BLOCK_SIZE ? (direct / BLOCK_SIZE + 1) : (direct / BLOCK_SIZE);
    uint32_t rest = direct,i;
    //统一先direct,不够再indirect
    for (i = 0; i < direct_size; ++i) {
        uint32_t written = rest > BLOCK_SIZE ? BLOCK_SIZE : rest;
        uint32_t id=node->blocks[i];
        if (id > 0) {
            //此处一定要先拷到空数组,再write_block,否则容易溢出 20211011
            char tmp[BLOCK_SIZE];
            memset(tmp, 0, BLOCK_SIZE);
            memcpy(tmp, buf + i * BLOCK_SIZE, written);
            bm->write_block(id, tmp);
        } else {
            node->blocks[i]=bm->alloc_block();
            if(node->blocks[i]!=0) {
                char tmp[BLOCK_SIZE];
                bm->read_block(node->blocks[i],tmp);
                memcpy(tmp,buf + i * BLOCK_SIZE, written);
                bm->write_block(node->blocks[i],tmp);
            }
        }
        rest -= written;
    }
    for (; i < NDIRECT; ++i) {
        if (node->blocks[i] > 0) {
//                std::cout<<"inode_manager inum="<<inum<<" free_block:"<<node->blocks[i]<<std::endl;
            bm->free_block(node->blocks[i]);
            node->blocks[i] = 0;
        }
    }
    uint32_t indirect = size - direct;
    //先存ndirect里指向的目录,后面遍历
    uint32_t old_id = node->blocks[NDIRECT];
    if (old_id > 0) {
        char tmp[BLOCK_SIZE];
        bm->read_block(old_id, tmp);
        int offset = 0;
        uint32_t id;
        while ((id = read_bytes(tmp + 4 * offset))) {
            bm->free_block(id);
            offset++;
        }
        bm->free_block(old_id);
        node->blocks[NDIRECT] = 0;
    }

    //direct不够放 放满direct 再到indirect里放
    if (indirect > 0) {
        old_id = bm->alloc_block();
        node->blocks[NDIRECT] = old_id;
        direct_size = indirect % BLOCK_SIZE ? (indirect / BLOCK_SIZE + 1) : (indirect / BLOCK_SIZE);
        char tmp[BLOCK_SIZE];
        memset(tmp, 0, BLOCK_SIZE);
        rest = indirect;
        for (i = 0; i < direct_size; ++i) {
            uint32_t written = rest > BLOCK_SIZE ? BLOCK_SIZE : rest;
            uint32_t id=bm->alloc_block();
            if(id!=0) {
                char tmp[BLOCK_SIZE];
                bm->read_block(id,tmp);
                memcpy(tmp,buf + (NDIRECT + i) * BLOCK_SIZE,written);
                bm->write_block(id,tmp);
            }
            write_bytes(&tmp[i * 4], id);
            rest -= written;
        }
        bm->write_block(old_id, tmp);
    }
//    std::cout<<"inode_manager inum="<<inum<<" write_block completed!"<<std::endl;

    put_inode(inum, node);
    delete node;
    node=nullptr;
    return;
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a) {
    /*
     * your code goes here.
     * note: get the attributes of inode inum.
     * you can refer to "struct attr" in extent_protocol.h
     */
    inode *node = get_inode(inum);
    if (node == NULL) {
        return;
    }
    a.type = node->type;
    a.atime = node->atime;
    a.mtime = node->mtime;
    a.ctime = node->ctime;
    a.size = node->size;
    delete node;
    node = nullptr;
    return;
}

void inode_manager::remove_file(uint32_t inum) {
    /*
     * your code goes here
     * note: you need to consider about both the data block and inode of the file
     */
    inode *node = get_inode(inum);
//    std::cout << "inode_manager::remove_file called inum=" << inum << "node->size=" << node->size << std::endl;
    if (node == NULL || node->size == 0) {
        free_inode(inum);
        delete node;
        node = nullptr;
        return;
    }
    if (node->size <= NDIRECT * BLOCK_SIZE) {
        int index = (node->size - 1) / BLOCK_SIZE + 1;
//        std::cout << "inode_manager::remove_file called inum=" << inum << " index=" << index << std::endl;
        for (int i = 0; i < index; i++) {
//            std::cout << "inode_manager::remove_file i=" << i << " blocks[i]=" << node->blocks[i] << std::endl;
            bm->free_block(node->blocks[i]);
        }
    } else {
        for (int i = 0; i < NDIRECT; i++) {
            bm->free_block(node->blocks[i]);
        }
        uint32_t index[BLOCK_SIZE];
        char buf[BLOCK_SIZE];
        bm->read_block(node->blocks[NDIRECT], buf);
        memcpy(index, buf, BLOCK_SIZE);
        int indirect = (node->size - NDIRECT * BLOCK_SIZE - 1) / BLOCK_SIZE + 1;
        for (int i = 0; i < indirect; i++) {
            bm->free_block(index[i]);
        }
        bm->free_block(node->blocks[NDIRECT]);
    }
    free_inode(inum);
    delete node;
    node = nullptr;
    return;
}
