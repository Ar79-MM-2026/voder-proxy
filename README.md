voder-proxy
===========

Intercept requests to `voder/tts_service` and determine the appropriate
backend to send them to. This acts as a load balancer of sorts.

make_service_fn ->

  service_fn(|req : Request<Body>| -> ServiceFn<F,R>)
    It can return: 
      - Ok(Response::new(Body::from("whatevs)))
      - Err("some explanation")
  
   I'm doing matching on the route and method,
   
   speak_proxy(req: Request<Body>, ... other args ...) -> BoxFut
   
     where BoxFut = Box<dyn Future<Item=Response<Body>, Error=hyper::Error> + Send>
   
     this also calls,
     
   hyper_reverse_proxy::call(
       client_ip: IpAddr, 
       forward_uri: &str, 
       request: Request<Body>) -> BoxFut
     
   Future is from futures-0.1.29
   
     boxed() -> BoxFut<> + Send
        * Use Box::new() to get around Send
     
     map() can turn Future<Item=T> into Future<Item=U>
       * Only called on success
       * Consumes the future
        * future::ok(1).map(|x| x+3) -> Ok(4)
       
     map_err() 
        * Only called on error
        * future::err(1).map_err(|x| x+3) -> Err(4)
     
     and_then() can create a future(?)
     
     then() runs the future, no matter what
        * Runs over Result<> type
        * Returns `IntoFuture` trait type; Result has this
        * future::ok(1).then(|x| match x { OK() => {} Err => { future::ok(...) } }
     
     and_then() execute another future 
        * Runs over Ok()/Item type.
     
     and_then() execute another future in success case
        * Runs over Ok()/Item type.
     
     or_else() execute another future in fail case 
        * Runs over Err()/Error type.
        
     inspect() look at the future, but pass it on!
