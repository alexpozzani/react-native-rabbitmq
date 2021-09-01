#import "RabbitMqConnection.h"
#import "EventEmitter.h"
#import <RMQClient/RMQConnection.h>

@interface RabbitMqQueue : NSObject <RCTBridgeModule>

@property (nonatomic, readwrite) NSString *name;
@property (nonatomic, readwrite) NSDictionary *config;
@property (nonatomic, readwrite) RMQQueue *queue;
@property (nonatomic, readwrite) id<RMQChannel> channel;
@property (nonatomic, readwrite) RMQQueueDeclareOptions options;
@property (nonatomic, readwrite) RCTBridge *bridge;
@property (nonatomic, readwrite) RMQConsumer *consumer;

    - (nonnull id) initWithConfig:(nonnull NSDictionary *)config
                          channel:(nonnull id)channel;

    - (void) bind:(nonnull id)exchange routing_key:(NSString *)routing_key;
    - (void) unbind:(nonnull id)exchange routing_key:(NSString *)routing_key;
    - (void) delete;
    - (void) ack: (NSNumber *)deliveryTag;
    - (void) cancelConsumer;
@end

@implementation RabbitMqQueue


RCT_EXPORT_MODULE();

-(id)initWithConfig:(NSDictionary *)config channel:(id<RMQChannel>)channel {
    if (self = [super init]) {

        self.config = config;
        self.channel = channel;
        self.name = [config objectForKey:@"name"];

        self.options = RMQQueueDeclareNoOptions;

        if ([config objectForKey:@"passive"] != nil && [[config objectForKey:@"passive"] boolValue]){
            self.options = self.options | RMQQueueDeclarePassive;
        }

        if ([config objectForKey:@"durable"] != nil && [[config objectForKey:@"durable"] boolValue]){
           self.options = self.options | RMQQueueDeclareDurable;
        }

        if ([config objectForKey:@"exclusive"] != nil && [[config objectForKey:@"exclusive"] boolValue]){
            self.options = self.options | RMQQueueDeclareExclusive;
        }

        if ([config objectForKey:@"autoDelete"] != nil && [[config objectForKey:@"autoDelete"] boolValue]){
            self.options = self.options | RMQExchangeDeclareAutoDelete;
        }

        if ([config objectForKey:@"NoWait"] != nil && [[config objectForKey:@"NoWait"] boolValue]){
            self.options = self.options | RMQQueueDeclareNoWait;
        }

        self.queue = [self.channel queue:self.name options:self.options];


        NSMutableDictionary *tmp_arguments = [[NSMutableDictionary alloc]init];
        if ([config objectForKey:@"consumer_arguments"] != nil){

            NSDictionary *consumer_arguments = [config objectForKey:@"consumer_arguments"];

            if ([consumer_arguments objectForKey:@"x-message-ttl"] != nil){
                NSNumber *ttl = [consumer_arguments objectForKey:@"x-message-ttl"];
                [tmp_arguments setObject:[[RMQSignedShort alloc] init:[ttl integerValue]] forKey:@"x-message-ttl"];
            }

            if ([consumer_arguments objectForKey:@"x-priority"] != nil){
                NSNumber *xpriority = [consumer_arguments objectForKey:@"x-priority"];
                [tmp_arguments setObject:[[RMQSignedShort alloc] init:[xpriority integerValue]] forKey:@"x-priority"];
            }

            if ([consumer_arguments objectForKey:@"x-single-active-consumer"] != nil){
                BOOL active = [[consumer_arguments valueForKey:@"x-single-active-consumer"] boolValue];
                [tmp_arguments setObject:[[RMQBoolean alloc] init:active] forKey:@"x-single-active-consumer"];
            }

        }

        RMQBasicConsumeOptions consumer_options = RMQBasicConsumeNoOptions;

        RMQTable *arguments = [[RMQTable alloc] init:tmp_arguments];

        if ([config objectForKey:@"consume"] != nil && [[config objectForKey:@"consume"] boolValue]){
			self.consumer = [self.queue subscribe:consumer_options
						arguments:arguments
						handler:^(RMQMessage * _Nonnull message) {

				NSString *body = [[NSString alloc] initWithData:message.body encoding:NSUTF8StringEncoding];
				if (body == nil) {
					NSStringEncoding gbkEncoding = CFStringConvertEncodingToNSStringEncoding(kCFStringEncodingGB_18030_2000);
					body = [[NSString alloc] initWithData:message.body encoding:gbkEncoding];
				}

				if (body == nil) {
					body = @"编码解析出错";
				}


				// [message.body utf8String];

				//[self.channel ack:message.deliveryTag];

				[EventEmitter emitEventWithName:@"RabbitMqQueueEvent"
					body:@{
						@"name": @"message",
						@"queue_name": self.name,
						@"message": body,
						@"routingKey": message.routingKey, // Will be deprecated
						@"routing_key": message.routingKey,
						@"exchange": message.exchangeName,
						@"consumer_tag": message.consumerTag,
						@"delivery_tag": message.deliveryTag
					}
				];

			}];
		}



    }
    return self;
}

-(void) bind:(RMQExchange *)exchange routing_key:(NSString *)routing_key {


    if ([routing_key length] == 0){
        [self.queue bind:exchange];
    }else{
        [self.queue bind:exchange routingKey:routing_key];
    }
}

-(void) unbind:(RMQExchange *)exchange routing_key:(NSString *)routing_key {

    if ([routing_key length] == 0){
        [self.queue unbind:exchange];
    }else{
        [self.queue unbind:exchange routingKey:routing_key];
    }
}

-(void) delete {
    [self.queue delete:self.options];
}

-(void) ack:(NSNumber *)deliveryTag {
    [self.channel ack:deliveryTag];
}

-(void) cancelConsumer {
    [self.consumer cancel];
}

@end

@interface RabbitMqDelegateLogger : NSObject<RMQConnectionDelegate>

@end

@implementation RabbitMqDelegateLogger

-(id)init {
    self = [super init];
    return self;
}

- (void)connection:(id)connection failedToConnectWithError:(NSError *)error {
    NSLog(@"RabbitMq Received connection: %@ failedToConnectWithError: %@", connection, error);
    [EventEmitter emitEventWithName:@"RabbitMqConnectionEvent" body:@{@"name": @"error", @"type": @"failedtoconnect", @"code": [NSString stringWithFormat:@"%ld", error.code], @"description": [error localizedDescription]}];
}

- (void)connection:(id)connection disconnectedWithError:(NSError *)error {
    NSLog(@"RabbitMq Received connection: %@ disconnectedWithError: %@", connection, error);
    [EventEmitter emitEventWithName:@"RabbitMqConnectionEvent" body:@{@"name": @"error", @"type": @"disconnected",  @"code": [NSString stringWithFormat:@"%ld", error.code], @"description": [error localizedDescription]}];
}

- (void)channel:(id)channel error:(NSError *)error {
    NSLog(@"RabbitMq Received channel: %@ error: %@", channel, error);
    [EventEmitter emitEventWithName:@"RabbitMqConnectionEvent" body:@{@"name": @"error", @"type": @"channel",  @"code": [NSString stringWithFormat:@"%ld", error.code], @"description": [error localizedDescription]}];
}

- (void)willStartRecoveryWithConnection:(id)connection {
    NSLog(@"RabbitMq Will start recovery for connection: %@", connection);
}

- (void)startingRecoveryWithConnection:(id)connection {
    NSLog(@"RabbitMq Starting recovery for connection: %@", connection);
}

- (void)recoveredConnection:(id)connection {
    NSLog(@"RabbitMq Recovered connection: %@", connection);
    [EventEmitter emitEventWithName:@"RabbitMqConnectionEvent" body:@{@"name": @"reconnected"}];
}

@end



@interface RabbitMqConnection ()
    @property (nonatomic, readwrite) NSDictionary *config;
    @property (nonatomic, readwrite) RMQConnection *connection;
    @property (nonatomic, readwrite) id<RMQChannel> channel;
    @property (nonatomic, readwrite) bool connected;
    @property (nonatomic, readwrite) NSMutableArray *queues;
    @property (nonatomic, readwrite) NSMutableArray *exchanges;
@end

@implementation RabbitMqConnection

@synthesize bridge = _bridge;

RCT_EXPORT_MODULE();

RCT_EXPORT_METHOD(initialize:(NSDictionary *) config)
{
    self.config = config;

    self.connected = false;

    self.queues = [[NSMutableArray alloc] init];
    self.exchanges = [[NSMutableArray alloc] init];
}

RCT_EXPORT_METHOD(connect)
{

    RabbitMqDelegateLogger *delegate = [[RabbitMqDelegateLogger alloc] init];

    NSString *protocol = @"amqp";
    if ([self.config objectForKey:@"ssl"] != nil && [[self.config objectForKey:@"ssl"] boolValue]){
        protocol = @"amqps";
    }

    NSString *uri = [NSString stringWithFormat:@"%@://%@:%@@%@:%@/%@", protocol, self.config[@"username"], self.config[@"password"], self.config[@"host"], self.config[@"port"], self.config[@"virtualhost"]];


    self.connection = [[RMQConnection alloc] initWithUri:uri
                                              channelMax:@65535
                                                frameMax:@(131072)
                                               heartbeat:@10
										  connectTimeout:@15
											 readTimeout:@30
										    writeTimeout:@30
                                             syncTimeout:@10
                                                delegate:delegate
                                           delegateQueue:dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0)];

    [self.connection start:^{

        self.connected = true;

        [EventEmitter emitEventWithName:@"RabbitMqConnectionEvent" body:@{@"name": @"connected"}];

    }];
}

RCT_EXPORT_METHOD(close)
{
    for(id q in self.queues) {
        [q cancelConsumer];
    }

    [self.connection close];
    [self.queues removeAllObjects];
    [self.exchanges removeAllObjects];

    self.connection = nil;
}

RCT_EXPORT_METHOD(addQueue:(NSDictionary *) config arguments:(NSDictionary *)arguments)
{
    if (self.connected){
        self.channel = [self.connection createChannel];

        RabbitMqQueue *queue = [[RabbitMqQueue alloc] initWithConfig:config channel:self.channel];

        [self.queues addObject:queue];
    }
}

RCT_EXPORT_METHOD(bindQueue:(NSString *)exchange_name queue_name:(NSString *)queue_name routing_key:(NSString *)routing_key)
{

    id queue_id = [self findQueue:queue_name];
    id exchange_id = [self findExchange:exchange_name];

    if (queue_id != nil && exchange_id != nil){
        [queue_id bind:exchange_id routing_key:routing_key];
    }

}

RCT_EXPORT_METHOD(unbindQueue:(NSString *)exchange_name queue_name:(NSString *)queue_name routing_key:(NSString *)routing_key)
{
    id queue_id = [self findQueue:queue_name];
    id exchange_id = [self findExchange:exchange_name];

    if (queue_id != nil && exchange_id != nil){
        [queue_id unbind:exchange_id routing_key:routing_key];
    }
}

RCT_EXPORT_METHOD(removeQueue:(NSString *)queue_name)
{
    id queue_id = [self findQueue:queue_name];

    if (queue_id != nil){
        [queue_id delete];
    }
}

RCT_EXPORT_METHOD(basicAck:(NSString *)queue_name delivery_tag:(nonnull NSNumber *)delivery_tag)
{
    id queue_id = [self findQueue:queue_name];

    if (queue_id != nil){
        [queue_id ack:delivery_tag];
    }
}

RCT_EXPORT_METHOD(cancelConsumer:(NSString *)queue_name)
{
    id queue_id = [self findQueue:queue_name];

    if (queue_id != nil){
        [queue_id cancelConsumer];
    }
}

RCT_EXPORT_METHOD(addExchange:(NSDictionary *) config)
{

    RMQExchangeDeclareOptions options = RMQExchangeDeclareNoOptions;

    if ([config objectForKey:@"passive"] != nil && [[config objectForKey:@"passive"] boolValue]){
        options = options | RMQExchangeDeclarePassive;
    }

    if ([config objectForKey:@"durable"] != nil && [[config objectForKey:@"durable"] boolValue]){
        options = options | RMQExchangeDeclareDurable;
    }

    if ([config objectForKey:@"autoDelete"] != nil && [[config objectForKey:@"autoDelete"] boolValue]){
        options = options | RMQExchangeDeclareAutoDelete;
    }

    if ([config objectForKey:@"internal"] != nil && [[config objectForKey:@"internal"] boolValue]){
        options = options | RMQExchangeDeclareInternal;
    }

    if ([config objectForKey:@"NoWait"] != nil && [[config objectForKey:@"NoWait"] boolValue]){
        options = options | RMQExchangeDeclareNoWait;
    }

    NSString *name = [config objectForKey:@"name"];
    if ([type isEqualToString:@""]) {
	} else {
		NSString *type = [config objectForKey:@"type"];

		RMQExchange *exchange = nil;
		if ([type isEqualToString:@"fanout"]) {
			exchange = [self.channel fanout:[config objectForKey:@"name"] options:options];
		}else if ([type isEqualToString:@"direct"]) {
			exchange = [self.channel direct:[config objectForKey:@"name"] options:options];
		}else if ([type isEqualToString:@"topic"]) {
			exchange = [self.channel topic:[config objectForKey:@"name"] options:options];
		}

		if (exchange != nil){
			[self.exchanges addObject:exchange];
		}
	}
}

RCT_EXPORT_METHOD(publishToExchange:(NSString *)message exchange_name:(NSString *)exchange_name routing_key:(NSString *)routing_key message_properties:(NSDictionary *)message_properties)
{

    id exchange_id = [self findExchange:exchange_name];

    if (exchange_id != nil){

        NSData* data = [message dataUsingEncoding:NSUTF8StringEncoding];

        NSMutableArray *properties = [[NSMutableArray alloc] init];

        if ([message_properties objectForKey:@"content_type"] != nil && [[message_properties objectForKey:@"content_type"] isMemberOfClass:[NSString class]]){
            [properties addObject:[[RMQBasicContentType alloc] init:[[message_properties objectForKey:@"content_type"] stringValue]]];
        }
        if ([message_properties objectForKey:@"content_encoding"] != nil && [[message_properties objectForKey:@"content_encoding"] isMemberOfClass:[NSString class]]){
            [properties addObject:[[RMQBasicContentEncoding alloc] init:[[message_properties objectForKey:@"content_encoding"] stringValue]]];
        }
        if ([message_properties objectForKey:@"delivery_mode"] != nil){
            [properties addObject:[[RMQBasicDeliveryMode alloc] init:[[message_properties objectForKey:@"delivery_mode"] intValue]]];
        }
        if ([message_properties objectForKey:@"priority"] != nil){
            [properties addObject:[[RMQBasicPriority alloc] init:[[message_properties objectForKey:@"priority"] intValue]]];
        }
        if ([message_properties objectForKey:@"correlation_id"] != nil && [[message_properties objectForKey:@"correlation_id"] isMemberOfClass:[NSString class]]){
            [properties addObject:[[RMQBasicCorrelationId alloc] init:[[message_properties objectForKey:@"correlation_id"] stringValue]]];
        }
        if ([message_properties objectForKey:@"expiration"] != nil && [[message_properties objectForKey:@"expiration"] isMemberOfClass:[NSString class]]){
            [properties addObject:[[RMQBasicExpiration alloc] init:[[message_properties objectForKey:@"expiration"] stringValue]]];
        }
        if ([message_properties objectForKey:@"message_id"] != nil && [[message_properties objectForKey:@"message_id"] isMemberOfClass:[NSString class]]){
            [properties addObject:[[RMQBasicMessageId alloc] init:[[message_properties objectForKey:@"message_id"] stringValue]]];
        }
        if ([message_properties objectForKey:@"type"] != nil && [[message_properties objectForKey:@"type"] isMemberOfClass:[NSString class]]){
            [properties addObject:[[RMQBasicType alloc] init:[[message_properties objectForKey:@"type"] stringValue]]];
        }
        if ([message_properties objectForKey:@"user_id"] != nil && [[message_properties objectForKey:@"user_id"] isMemberOfClass:[NSString class]]){
            [properties addObject:[[RMQBasicUserId alloc] init:[[message_properties objectForKey:@"user_id"] stringValue]]];
        }
        if ([message_properties objectForKey:@"app_id"] != nil && [[message_properties objectForKey:@"app_id"] isMemberOfClass:[NSString class]]){
            [properties addObject:[[RMQBasicAppId alloc] init:[[message_properties objectForKey:@"app_id"] stringValue]]];
        }
        if ([message_properties objectForKey:@"reply_to"] != nil && [[message_properties objectForKey:@"reply_to"] isMemberOfClass:[NSString class]]){
            [properties addObject:[[RMQBasicReplyTo alloc] init:[[message_properties objectForKey:@"reply_to"] stringValue]]];
        }

        [exchange_id publish:data routingKey:routing_key properties:properties options:RMQBasicPublishNoOptions];
    }
}

RCT_EXPORT_METHOD(deleteExchange:(NSString *)exchange_name)
{
   id exchange_id = [self findExchange:exchange_name];

    if (exchange_id != nil){
        [exchange_id delete];
    }
}



-(id) findQueue:(NSString *)name {
    id queue_id = nil;
    for(id q in self.queues) {
        if ([[q name] isEqualToString:name]){ queue_id = q; }
    }
    return queue_id;
}

-(id) findExchange:(NSString *)name {
    id exchange_id = nil;
    for(id e in self.exchanges) {
        if ([[e name] isEqualToString:name]){ exchange_id = e; }
    }

    return exchange_id;
}

@end
