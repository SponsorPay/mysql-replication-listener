#include "access_method_factory.h"
#include "tcp_driver.h"

namespace MySQL { 
system::Binary_log_driver *create_transport(const char *url)
{
  // TODO Parse the URL (using boost::spirit) and call the proper device driver
  //      factory.
  // temporary code for test purpose follows below
    char *url_copy= strdup(url);
    const char *access_method= "mysql://";
    char user[21];
    char host[50];
    char port[6];
    char pass[21];
    user[0]= 0;
    host[0]= 0;
    port[0]= 0;
    pass[0]= 0;
    unsigned user_length= 0;
    unsigned host_length= 0;
    unsigned port_length= 0;
    unsigned pass_length= 0;
    unsigned long portno= 0;
    char *front, *scan;
    front= strstr(url_copy,access_method);
    if (front == 0)
        goto err;
    front += strlen(access_method);

    scan= strpbrk(front,":@");
    if (scan == 0)
        goto err;
    user_length= scan - front;
    if (user_length > 20)
        goto err;
    memcpy(user, front, user_length);
    user[user_length]= '\0';
    front = scan;
    if (*front == ':')
    {
        front += 1;
        scan= strpbrk(front, "@");
        if (scan == 0)
            goto err;
        pass_length= scan - front;
        memcpy(pass, front, pass_length);
        pass[pass_length] = '\0';
        front = scan;
    }

    if (*front != '@')
        goto err;
    front += 1;
    scan= strpbrk(front,":");
    if (scan != 0)
        host_length= scan - front;
    else
        host_length= strlen(front);

    if (host_length > 49)
        goto err;
    memcpy(host,front, host_length);
    host[host_length]= '\0';
    front= scan;
    if (front && *front == ':')
    {
        front +=1;
        port_length= strlen(front);
        if (port_length > 5)
            goto err;
        memcpy(port, front, port_length);
        port[port_length]= '\0';
        portno= atol(port);
    }

    // Only tcp driver supported so far
    free(url_copy);
    return new system::Binlog_tcp_driver(user, pass, host, portno);


err:
    free(url_copy);
    return 0;
}

} // end namespace


