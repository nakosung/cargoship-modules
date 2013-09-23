_ = require 'underscore'
events = require 'events'
async = require 'async'
es = require 'event-stream'
log = require 'winston'
dnode = require 'dnode'

module.exports = ->
	RETRY_INTERVAL = 1000
	REQUIRE_TIMEOUT = 3000

	modules = (m,next) -> next m
	modules.signature = 'modules'

	external_service = (ship) ->
		mxs = []
		ship.on 'connect', (mx) -> 
			mxs.push mx
			mx.upstream.once 'end', ->
				mxs = _.without mxs, mx

		external_service = (a,next) ->
			connect_with = (mx) ->
				log.info "connecting #{a}"

				[service,meta] = a.split(':')			

				meta = 
					user : ship.user
					meta : meta	

				s = mx.createStream [service,JSON.stringify(meta)].join(':')
				d = dnode()
				es.pipeline(s,d,s).once 'error', (e) ->
					console.trace "pipeline error #{a}", String(e)
					s.end()
				s.once 'end', ->
					log.error "connection dropped #{a}"
					next('end')
				d.once 'remote', (r) ->
					log.info "connected to #{a}"
					next(null,r)

			connect = ->
				if mxs.length
					connect_with mxs[0]
				else
					log.info "no servers found waiting!"
					ship.once 'connect', (mx) ->
						log.info "a server found"
						connect_with mx
				
			connect()

		external_service

	modules.preuse = (ship) ->
		repo = {}
		patterns = []
		ship.register = (name,func) ->
			if _.isRegExp(name)
				patterns.push 
					regexp : name
					func : func
			else
				repo[name] = func
		cache = {}	
		default_handler = external_service(ship)

		ship.methods = (methods) ->
			ship.use (m) ->
				M = {}			
				for k,v of methods
					M[k] = v.bind(m)
				d = dnode M
				es.pipeline(d,m,d).once 'error', (e) ->
					log.error String(e)
					m.end()

		require_events = new events.EventEmitter()			
		ship.require = (args...,next) ->	
			jobs = args.map (a) ->
				(next) ->
					C = cache[a]
					return next null, C.module if C?.valid
					return C.wait next if C?

					C = cache[a] = new events.EventEmitter()
					C.once 'online', ->	require_events.emit 'online', a, C.module
					C.once 'offline', -> require_events.emit 'offline', a, C.module
					C.waiting = 0
					C.wait = (next) ->
						C.waiting++
						C.once 'online', ->							
							C.waiting--
							next null, C.module
						C.once 'offline', ->
							C.waiting--
							next 404

					C.wait next

					resolve = ->
						log.info 'resolve', a
						next = (err,r) ->
							if err							
								if C.valid
									if C.waiting
										C.valid = false
										setTimeout resolve, RETRY_INTERVAL
									else
										delete cache[a]
								else
									C.emit 'offline'
									C.removeAllListeners()
									delete cache[a]
							else
								C.valid = true
								C.module = r
								C.emit 'online'

						R = repo[a]
						unless R?
							P = _.find patterns, (p) -> p.regexp.test(a)
							R = P?.func or default_handler
						R.call ship, a, next					
					resolve()
			_.extend ship.require, require_events

			timedout = false
			timer = setTimeout (->			
				timedout = true
				log.error 'timed out!'
				next 'timeout'
				), REQUIRE_TIMEOUT

			async.parallel jobs, (err,result) ->
				clearTimeout timer
				next err, result... unless timedout

	modules
