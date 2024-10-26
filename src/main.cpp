#include <hiredis/hiredis.h>
#include <iostream>

int main() {
    // reids connection
    redisContext *c = redisConnect("127.0.0.1", 6379);
    if (c == nullptr || c->err) {
        if (c) {
            std::cerr << "Connection error: " << c->errstr << std::endl;
            redisFree(c);
        } else {
            std::cerr << "Connection error: can't allocate redis context" << std::endl;
        }
        return 1;
    }

    // tst connection
    redisReply *reply = (redisReply *)redisCommand(c, "PING");
    std::cout << "PING: " << reply->str << std::endl;
    freeReplyObject(reply);

    // disconnect
    redisFree(c);
    return 0;
}
