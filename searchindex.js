Search.setIndex({envversion:50,filenames:["adapters","api/baseadapter","api/baseexecutor","api/config","api/eva","api/eventloop","api/eventqueue","api/exceptions","api/index","configuration","development","index","intro","metrics","restapi","tutorial","variables"],objects:{"":{eva:[4,0,0,"-"]},"eva.adapter":{CWFAdapter:[0,1,1,""],ChecksumVerificationAdapter:[0,1,1,""],DeleteAdapter:[0,1,1,""],DistributionAdapter:[0,1,1,""],ExampleAdapter:[0,1,1,""],FimexAdapter:[0,1,1,""],FimexFillFileAdapter:[0,1,1,""],FimexGRIB2NetCDFAdapter:[0,1,1,""],GridPPAdapter:[0,1,1,""],NowcastPPAdapter:[0,1,1,""],NullAdapter:[0,1,1,""],TestExecutorAdapter:[0,1,1,""],ThreddsAdapter:[0,1,1,""]},"eva.base":{adapter:[1,0,0,"-"],executor:[2,0,0,"-"]},"eva.base.adapter":{BaseAdapter:[1,1,1,""]},"eva.base.adapter.BaseAdapter":{adapter_init:[1,2,1,""],api:[1,3,1,""],blacklist_add:[1,2,1,""],clear_required_uuids:[1,2,1,""],concurrency:[1,3,1,""],create_job:[1,2,1,""],create_logger:[1,2,1,""],datainstance_has_required_uuids:[1,2,1,""],default_resource_dictionary:[1,4,1,""],executor:[1,3,1,""],expiry_from_hours:[1,2,1,""],expiry_from_lifetime:[1,2,1,""],finish_job:[1,2,1,""],forward_to_uuid:[1,2,1,""],generate_and_post_resources:[1,2,1,""],generate_resources:[1,2,1,""],has_output_lifetime:[1,2,1,""],has_productstatus_credentials:[1,2,1,""],init:[1,2,1,""],is_blacklisted:[1,2,1,""],is_in_required_uuids:[1,2,1,""],post_resources:[1,2,1,""],post_to_productstatus:[1,2,1,""],reference_time_threshold:[1,2,1,""],remove_required_uuid:[1,2,1,""],require_productstatus_credentials:[1,2,1,""],resource_matches_hash_config:[1,2,1,""],resource_matches_input_config:[1,2,1,""],validate_resource:[1,2,1,""]},"eva.base.executor":{BaseExecutor:[2,1,1,""]},"eva.base.executor.BaseExecutor":{abort:[2,2,1,""],create_temporary_script:[2,2,1,""],delete_temporary_script:[2,2,1,""],execute_async:[2,2,1,""],sync:[2,2,1,""]},"eva.config":{ConfigurableObject:[3,1,1,""],ResolvableDependency:[3,1,1,""],SECRET_CONFIGURATION:[3,7,1,""],resolved_config_section:[3,5,1,""]},"eva.config.ConfigurableObject":{CONFIG:[3,3,1,""],OPTIONAL_CONFIG:[3,3,1,""],REQUIRED_CONFIG:[3,3,1,""],_factory:[3,2,1,""],config_id:[3,3,1,""],factory:[3,6,1,""],format_config:[3,2,1,""],init:[3,2,1,""],isset:[3,2,1,""],load_configuration:[3,2,1,""],normalize_config_bool:[3,4,1,""],normalize_config_config_class:[3,4,1,""],normalize_config_float:[3,4,1,""],normalize_config_int:[3,4,1,""],normalize_config_list:[3,4,1,""],normalize_config_list_int:[3,4,1,""],normalize_config_list_string:[3,4,1,""],normalize_config_null_bool:[3,4,1,""],normalize_config_positive_int:[3,4,1,""],normalize_config_string:[3,4,1,""],resolve_dependencies:[3,2,1,""],set_config_id:[3,2,1,""]},"eva.config.ResolvableDependency":{resolve:[3,2,1,""]},"eva.eventloop":{Eventloop:[5,1,1,""]},"eva.eventloop.Eventloop":{adapter_by_config_id:[5,2,1,""],assert_event_matches_object_version:[5,2,1,""],create_event_queue_timer:[5,2,1,""],create_job_for_event_queue_item:[5,2,1,""],create_jobs_for_event_queue_item:[5,2,1,""],drained:[5,2,1,""],draining:[5,2,1,""],graceful_shutdown:[5,2,1,""],handle_kafka_error:[5,2,1,""],initialize_event_queue_item:[5,2,1,""],initialize_job:[5,2,1,""],instantiate_productstatus_data:[5,2,1,""],job_by_id:[5,2,1,""],main_loop_iteration:[5,2,1,""],must_the_show_go_on:[5,2,1,""],next_event_queue_item:[5,2,1,""],notify_job_failure:[5,2,1,""],notify_job_max_retry:[5,2,1,""],notify_job_recover:[5,2,1,""],poll_listeners:[5,2,1,""],process_all_in_product_instance:[5,2,1,""],process_data_instance:[5,2,1,""],process_job:[5,2,1,""],process_next_event:[5,2,1,""],process_rest_api:[5,2,1,""],reinitialize_job:[5,2,1,""],report_event_queue_metrics:[5,2,1,""],report_job_status_metrics:[5,2,1,""],reset_event_queue_item_generator:[5,2,1,""],restart_listeners:[5,2,1,""],restore_queue:[5,2,1,""],set_drain:[5,2,1,""],set_health_check_heartbeat_interval:[5,2,1,""],set_health_check_heartbeat_timeout:[5,2,1,""],set_health_check_skip_heartbeat:[5,2,1,""],set_health_check_timestamp:[5,2,1,""],set_message_timestamp_threshold:[5,2,1,""],set_no_drain:[5,2,1,""],shutdown:[5,2,1,""]},"eva.eventqueue":{EventQueue:[6,1,1,""],EventQueueItem:[6,1,1,""]},"eva.eventqueue.EventQueue":{adapter_active_job_count:[6,2,1,""],add_event:[6,2,1,""],delete_stored_item:[6,2,1,""],empty:[6,2,1,""],get_stored_queue:[6,2,1,""],init:[6,2,1,""],item_keys:[6,2,1,""],remove_item:[6,2,1,""],status_count:[6,2,1,""],store_item:[6,2,1,""],store_list:[6,2,1,""],store_serialized_data:[6,2,1,""],zk_get_serialized:[6,2,1,""],zk_get_str:[6,2,1,""],zk_immediate_store_disable:[6,2,1,""],zk_immediate_store_enable:[6,2,1,""]},"eva.eventqueue.EventQueueItem":{add_job:[6,2,1,""],empty:[6,2,1,""],failed_jobs:[6,2,1,""],finished:[6,2,1,""],id:[6,2,1,""],job_keys:[6,2,1,""],remove_job:[6,2,1,""],serialize:[6,2,1,""],set_adapters:[6,2,1,""]},"eva.exceptions":{AlreadyRunningException:[7,8,1,""],ConfigurationException:[7,8,1,""],DuplicateEventException:[7,8,1,""],EvaException:[7,8,1,""],EventTimeoutException:[7,8,1,""],GridEngineParseException:[7,8,1,""],InvalidConfigurationException:[7,8,1,""],InvalidEventException:[7,8,1,""],InvalidGroupIdException:[7,8,1,""],InvalidRPCException:[7,8,1,""],JobNotCompleteException:[7,8,1,""],JobNotGenerated:[7,8,1,""],MissingConfigurationException:[7,8,1,""],MissingConfigurationSectionException:[7,8,1,""],RPCException:[7,8,1,""],RPCFailedException:[7,8,1,""],RPCInvalidRegexException:[7,8,1,""],RPCWrongInstanceIDException:[7,8,1,""],ResourceTooOldException:[7,8,1,""],RetryException:[7,8,1,""],ShutdownException:[7,8,1,""],ZooKeeperDataTooLargeException:[7,8,1,""]},eva:{coerce_to_utc:[4,5,1,""],config:[3,0,0,"-"],convert_to_bytes:[4,5,1,""],epoch_with_timezone:[4,5,1,""],eventloop:[5,0,0,"-"],eventqueue:[6,0,0,"-"],exceptions:[7,0,0,"-"],format_exception_as_bug:[4,5,1,""],import_module_class:[4,5,1,""],in_array_or_empty:[4,5,1,""],log_productstatus_resource_info:[4,5,1,""],netcdf_time_to_timestamp:[4,5,1,""],now_with_timezone:[4,5,1,""],print_and_mail_exception:[4,5,1,""],retry_n:[4,5,1,""],split_comma_separated:[4,5,1,""],strftime_iso8601:[4,5,1,""],url_to_filename:[4,5,1,""],zookeeper_group_id:[4,5,1,""]}},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","method","Python method"],"3":["py","attribute","Python attribute"],"4":["py","staticmethod","Python static method"],"5":["py","function","Python function"],"6":["py","classmethod","Python class method"],"7":["py","data","Python data"],"8":["py","exception","Python exception"]},objtypes:{"0":"py:module","1":"py:class","2":"py:method","3":"py:attribute","4":"py:staticmethod","5":"py:function","6":"py:classmethod","7":"py:data","8":"py:exception"},terms:{"0fmkdgp":14,"1vlcf9h":14,"2a6af9dhiic":[],"2xx":14,"31t":[],"4pgu7disrx4bbvm0b4":14,"6joa0jh9ul9lcvx30qgpt":14,"7dfb76z9blritlb3ugb0ly":[],"7hvkdlokgkv37jh0":[],"7vj":14,"7xtifhga6dllbq8j":[],"8ttxswyspm":14,"90abcdef":14,"abstract":[],"boolean":3,"byte":[4,13],"case":[6,9],"catch":4,"class":[],"const":[],"default":[0,1,3,9,15,16],"delete":14,"export":[],"final":3,"float":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],"import":[1,4,10,15],"int":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],"long":[0,1,4,13],"new":[1,5,15,16],"null":[1,4],"return":[1,2,3,4,5,6],"static":[1,3],"throw":[3,4,6],"transient":[7,13],"true":[0,1,3,4,5,6,9,15,16],"try":[0,1,3,7],"var":[],"while":[3,6,14],__init__:4,_count:13,_factori:3,_size:13,abil:9,abl:1,abort:[2,13],about:[0,1,4,5,9,15],abov:14,absolut:1,accept:[0,1,5,9,12,13,14,15],access:15,accord:[0,3,7,15],across:[],action:1,activ:[6,10],actual:15,acymdeptrkvaaffya0hwxetijcexvuuf6aqvzyk6fo8f8qx4o:[],adapt:[],adapter_:[],adapter_active_job_count:6,adapter_by_config_id:5,adapter_init:[1,15],adaptive:[],add:[0,1,6,7,9],add_ev:6,add_job:6,adding:[],addit:6,addition:0,advanc:9,after:[0,1,2,3,5,7,13,15],afterward:2,again:[4,5,13],against:[1,13],agent:14,aihsec7:14,algorithm:15,alia:[],aliv:14,all:[],allow:[0,3,14],along:[0,3,14],alreadi:[0,3,6,7,13],alreadyrunningexcept:7,also:[1,14],alwai:16,amount:13,anatomi:[],ani:[0,1,3,5,13,14,15],anoth:[9,16],any:4,anyth:4,anywai:[],api_kei:3,appear:[],appli:16,applic:[9,14],apt:10,area:0,arg:4,argument:[4,14],around:6,arrai:[1,4],arriv:[0,7,13,15],ask:5,assert:[3,15],assert_event_matches_object_vers:5,assertionerror:[4,5],associ:[6,15],assum:[9,15],asynchron:[2,5,12],atmospher:0,attack:14,attempt:13,attr:[],attribut:15,authent:14,author:14,automat:[],auxiliari:0,avail:[0,3,12,14],avoid:9,awar:4,awk:15,axi:0,ayob2i99:[],back:[1,15],backend:[1,9],backtrac:4,bar:[3,16],bare:15,base:[],baseadapt:[1,5,6,15],baseexecutor:2,basename:3,basic:[9,14,15],batch:0,baz:3,bbcp:0,becaus:[7,10,13],been:[1,3,5,6,7,9,14],befor:[0,1,4,13],begin:14,behavior:[],belong:[0,5,13],below:3,between:[0,1,4,5],bg9szmfjztphymm:14,big:[7,9],bin:[0,10,14,15],binari:0,blacklist:1,blacklist_add:1,blacklist_uuid:[],blah:[],blrewhuxqywyjkxs77vmkp59bbtlnh153bzlbjulvmab:14,bool:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],both:9,brief:[1,2,5,6],broker:13,bug:[3,7],buggi:14,build:10,cach:[0,13,15],calcul:[0,1],call:[0,1,3,4,6,7,13,15],caller:4,can:[1,3,4,5,6,9,14,15],cannot:7,censor:3,certain:14,chang:[1,13,15],charset:14,check:[],checksum:[0,1,15],checksumadapt:15,checksumverificationadapt:[],child:[1,5],children:1,chronograph:12,chronolog:[],circumst:7,classmethod:3,clean:[2,4],clear_required_uuid:1,client:[],clobber:10,clone:10,code:[],coerc:[],coerce_to_utc:4,collect:6,com:[0,9,10],combin:3,come:12,comma:[0,3,4,9],command:[0,7,9,12,14,15],comment:9,commit:13,compil:[],complain:15,complet:[2,7,15],compon:[1,4],comput:15,concern:15,concurr:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],config:[],config_class:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],config_class_:[],config_id:[3,5],configpars:[3,7],configur:[],configurableobject:[1,3,15,16],configurationexcept:7,connect:[6,9,13,14],constant:[],consum:13,contain:[],content:[],contextu:[],continu:5,control:14,conveni:1,convers:[0,4,14],convert:[1,3,4],convert_to_byt:4,copi:[0,4],correct:[0,12],correctli:[1,14],cost:15,could:13,count:5,counter:13,counterpart:[0,13],cpu:13,cpython:14,crash:[6,13],creat:[],create_event_queue_tim:5,create_job:[1,15],create_job_for_event_queue_item:5,create_jobs_for_event_queue_item:5,create_logg:1,create_temporary_script:2,creation:0,credenti:1,criteria:[1,7],cross:9,current:[0,1,4,5,6,12,16],custom:[1,10,15],cw2jmlpzf9sljwlep5zkl8uhqbtg:14,cwf:0,cwf_domain:0,cwf_input_min_dai:0,cwf_lifetim:0,cwf_modul:0,cwf_nml_data_format:0,cwf_output_dai:0,cwf_output_directory_pattern:0,cwf_parallel:0,cwf_script_path:0,cwfadapter:[],daemon:[],dai:0,data:[0,1,3,5,6,7,13,14,15],data_inst:[],data_instance_uuid:5,databas:[0,15],dataformat:0,datainst:[0,1,5,13,15],datainstance_has_required_uuid:1,dataset:0,date:14,datetim:[1,4],dbzf4009es1a4nmyykqrs4w4ekiljsd:[],debug:6,dec:14,def:15,default_resource_dictionari:1,defin:[0,1,3,4,5,9,15,16],definit:[3,9],deflat:14,degre:14,delai:[],delet:[0,1,5,6,13],delete_batch_limit:0,delete_interval_sec:0,delete_stored_item:6,delete_temporary_script:2,deleteadapt:[],delimit:9,deliveri:15,demonstr:[],dep:10,depend:10,deprecated:[0,1],deriv:[1,3,16],describ:15,descript:[0,1,13],destin:0,detach:14,detail:[],detect:6,determin:1,dev:10,dict:[3,4,6],dictionari:[1,3,6,15],did:[7,13],differ:[3,5],direct:[],directli:12,directori:[0,9,10,15],disabl:6,disabled:15,discard:13,displai:[],distribut:0,distribution_destin:0,distribution_method:0,distribution_paramet:0,distributionadapt:[],doc:10,docker:[],doe:[1,3,4,7],domain:0,done:14,dot:[3,4,16],down:5,drain:[5,14],driven:15,due:[5,7,15],duplic:[1,9],duplicateeventexcept:[6,7],dure:0,e6alrmuvpqmpzckio8chrgcbhm6dm:14,each:[5,6,15],earlier:[],ecdis4cwf:0,echo:0,ecmwf:0,either:[0,1,4,5,9,10,13,15,16],elif:15,els:[14,15],email:5,emep:0,empti:[0,1,4,5,6],empty:3,enabl:[5,6,9,14,15],encode:14,end:[9,14],engin:2,engine:13,ensur:6,entir:9,entri:6,env:[3,15],environ:[],environment_vari:[],ephemer:6,epoch_with_timezon:4,error:[3,4,5,6,7,12,13,14],escal:4,eva:[],eva_adapter_count:13,eva_deleted_datainst:13,eva_event_accept:13,eva_event_dupl:13,eva_event_heartbeat:13,eva_event_productstatu:13,eva_event_queue_count:13,eva_event_receiv:13,eva_event_reject:13,eva_event_too_old:13,eva_event_version_unsupport:13,eva_grid_engine_qsub_delai:13,eva_grid_engine_ru_stim:13,eva_grid_engine_ru_utim:13,eva_grid_engine_run_tim:13,eva_input_with_hash:[],eva_job_failur:13,eva_job_status_chang:13,eva_job_status_count:13,eva_kafka_commit_fail:13,eva_kafka_no_brokers_avail:13,eva_md5sum_fail:13,eva_queue_order:[],eva_recoverable_except:13,eva_requeue_reject:13,eva_requeued_job:13,eva_resource_object_version_too_old:13,eva_restored_ev:13,eva_restored_job:13,eva_shutdown:13,eva_start:13,eva_zk_:13,eva_zookeeper_connection_loss:13,evaexcept:7,evaluatedresourc:1,even:0,event_uuid:6,eventqueu:[],eventqueueitem:[5,6],events_:[],eventtimeoutexcept:7,everi:[1,15,16],everyon:15,evid:15,exactli:[],exampl:[0,9,14,16],exampleadapt:[],except:[],exception:[],excruci:15,execut:[],execute_async:2,executor:[],exist:[0,3,4,6,7],exit:12,expect:[5,7,15],expir:0,expiri:1,expiry_from_hour:1,expiry_from_lifetim:1,explan:9,explicit:1,expos:14,express:7,extern:3,extract:0,factori:3,fail:[0,1,5,6,7,15],failed_job:6,failur:[4,6,13],fall:15,fals:[1,3,4,5,6],fast:5,faster:[],featur:14,few:16,field:0,fifo:[],file:[],filenam:[0,1],filesystem:13,fill:[0,2],filter:3,fimex:0,fimex_fill_fil:[],fimex_fill_file_ncfill_path:0,fimex_fill_file_template_directori:0,fimex_grib_to_netcdf:[],fimex_paramet:0,fimexadapt:[],fimexfillfileadapt:[],fimexgrib2netcdfadapt:[],find:9,finish:[0,1,5,6,10,15],finish_job:[1,15],first:[9,15,16],fit:7,flag:1,flexibl:0,flow:15,follow:[1,6,14,16],foo:[0,3,9,16],fooadapt:16,foobar:16,forecast:0,forev:1,format:[],format_config:3,format_exception_as_bug:4,format_help:[],forth:[1,9],forward:5,forward_to_uuid:1,found:[3,4,5],from:[0,1,3,4,5,6,7,9,12,13,15,16],frontend:0,full:[0,2,4,16],func:4,further:12,furthermor:14,futur:3,gaug:13,generate_and_post_resourc:1,generate_resourc:[1,15],geo:0,geograph:0,get:[2,10,14],get_stored_queu:6,gigabyt:4,git:10,github:[0,10],gitlab:0,give:[0,4,5],give_up:4,given:[0,1,5],global:[9,16],gmt:14,gnupg:14,good:0,gpg:14,gpg_key_id:14,grace:5,graceful_shutdown:5,grant:9,grib:[0,9],grid:13,grid_engin:9,grid_engine_hostnam:13,grid_engine_qnam:13,gridengin:[0,7],gridengineexecutor:[0,9],gridengineparseexcept:7,gridpp:0,gridpp_generic_opt:0,gridpp_input_opt:0,gridpp_mask_opt:0,gridpp_modul:0,gridpp_output_opt:0,gridpp_preprocess_script:0,gridpp_thread:0,gridppadapt:[],group:9,group_id:7,guarante:[1,3,6],gzip:14,had:13,handl:[6,14,15],handle_kafka_error:5,has_output_lifetim:1,has_productstatus_credenti:1,hash:[0,1,3,15],hash_command:15,hash_typ:[0,15],have:[0,1,3,6,9,13,15],header:14,health:[],health_check_serv:[],healthi:5,heartbeat:[5,13],help:[10,14,15],here:[1,9,15],highli:12,hmyosw1ebt:14,home:[0,9],host:[0,9,14],hour:[0,1],how:[0,1,4,13,15],hp0apowl:14,http:[],httpie:[],i7bzpg0tgtshhqp:14,id_rsa:[0,9],identifi:[],ignor:[1,3,9],ignore_default:3,immedi:[2,5],implement:[1,3],impli:9,implicit:9,implicitli:0,import_module_class:4,in_array_or_empti:4,includ:[],inclus:15,inclusion:[0,1],incom:1,incompat:7,incub:[3,9],indefinit:4,index:11,indic:3,individu:6,infinit:3,info:15,inform:[0,1,4,6,9,15],infrastructur:[12,15],inherit:[1,9,16],ini:[3,9,15],init:[1,3,6],initi:[1,3,5,6,15],initial:[1,15],initialize_event_queue_item:5,initialize_job:5,input:[0,1,3,12],input_:1,input_data_format:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],input_file_format:9,input_parti:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],input_product:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],input_reference_hour:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],input_service_backend:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],input_with_hash:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],insid:15,instal:10,instanc:[0,1,3,4,5,6,7,14,15],instance_id:7,instanti:[3,5,9,13,16],instantiat:[1,3,6,15],instantiate_productstatus_data:5,instead:4,instruct:[1,12],insuffici:15,intact:[],integ:3,interest:15,interfac:[0,12],intern:[9,13],interv:[1,4,5],introduct:[],introduction_:[],invalid:[3,7],invalidconfigurationexcept:[3,7,15],invalideventexcept:7,invalidgroupidexcept:[4,7],invalidrpcexcept:7,iqicbaabagagbqjyv6cfaaojeijrrk:[],iqicbaabagagbqjyv7nhaaojeijrrk:14,irrecover:13,irrevers:[],is_blacklist:1,is_in_required_uuid:1,iso8601:4,iso8601_compact:0,isset:3,item:[5,6,13],item_id:6,item_kei:6,iter:[5,6],itself:10,ivara:0,ixhxkayfbklemo26xf:[],job:[],job_by_id:5,job_id:6,job_kei:6,job_uuid:6,jobnotcompleteexcept:7,jobnotgener:7,join:[15,16],jqzre:14,json:[6,14],k2ebghpd:14,k3iftkzxfbapmdvtlfzet8lcretfv93zfe7rrsyfvcxygc9h6doeskwx53edyuej:14,kafka:[5,12,13,14],kazoo:6,keep:14,kei:[1,3,14],keyword:4,kicxhqkmk9xlxfgqtjxblge5oyewh:[],kilobyt:4,kind:13,know:[],known:0,kq4zs8vthurjifcacnszmuqb3xp7gti:[],ktapdvimxlo:14,kwarg:4,kxqqcoridj87j:14,lack:[],laip:14,larg:6,last:0,later:15,latest:[],latter:[],launch:[],lax1ivuloyknt8yajjjlx2k:[],leav:5,left:[],length:14,less:0,let:[9,15],level:9,lftjrdclhk4:14,lib:4,librari:[],lifetim:[0,1],lifo:[],like:[9,15],line:[0,4,9,15],list:[],list_int:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],list_str:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],list_string_:[],listen:[5,9,15],live:[1,15],load:[],load_configur:3,load_serialized_data:[],local:10,localhost:[9,14],locat:[0,9],log:[1,3,4,5,6,12],log_productstatus_resource_info:4,logger:[1,4],logic:[12,15],login:0,loglevel:4,lol:[],look:[3,9,15],loop:5,loss:13,lost:[4,13,14],lot:5,lsx02yhqqk1e6sotsmjaxe6se3dob48n:14,lustr:0,lytfqehvobyfdc0fao8hfpnjh9sb:14,machin:10,made:[3,14],mai:[1,3,5,14,15,16],mail:[4,5,9],mailer:[4,9],main:[5,12,15],main_loop_iter:5,maintain:[5,6],make:[5,7,10,12,14,15],manag:6,mani:[0,1,4,9],mark:[0,1,13,15],mask:0,match:[1,5,7,13],maximum:5,mbswtd5opp8mikweluasossbf:[],md5:[0,15],md5sum:[13,15],mean:[1,14,15],meaning:[1,15],measur:5,megabyt:4,memb:[],member:[3,4],merg:9,messag:[1,4,5,6,7,12,13,14,15],met:0,metadata:[],method:[0,3,6,15],metno:[0,10],metric:[],metric_bas:6,might:[0,3,5],minim:1,minimum:15,mirror:6,misbehav:14,mismatch:13,miss:[0,3,7],missingconfigurationexcept:[3,7],missingconfigurationsectionexcept:[3,7],mnbu580jx73nhy1w0f:14,model:0,modifi:15,modul:[],moment:[0,13],mon:14,monitor:14,more:[5,7,9,13],most:[0,1,9],much:5,multipl:[],multipli:1,must:[1,3,6,9,14,15,16],must_the_show_go_on:5,myclass:3,name:[0,1,3,4,9,16],ncdump:4,ncfill:0,neccessari:15,necessari:[0,3,6],need:[3,5,7,15],neg:1,neighbourhood:0,netcdf:[0,9],netcdf_time_to_timestamp:4,network:[7,13],never:1,newer:[],newli:[1,15],next:[7,15],next_event_queue_item:5,nipen:0,njrd:[],nml:0,node:[0,15],non:[],none:[1,3,5,6],noob:[],normal:3,normalize_config_:3,normalize_config_bool:3,normalize_config_config_class:3,normalize_config_float:3,normalize_config_int:3,normalize_config_list:3,normalize_config_list_int:3,normalize_config_list_str:3,normalize_config_null_bool:3,normalize_config_positive_int:3,normalize_config_str:3,nosetest:[],notat:[4,16],note:[3,6,9,15],noth:[1,3,15],notifi:5,notify_job_failur:5,notify_job_max_retri:5,notify_job_recov:5,notify_job_success:[],now:15,now_with_timezon:4,nowcast:0,nowcastpp:[],nowcastppadapt:[],nr53:14,nrlxhwppy2mq3tpt:[],nrpa_europe_0_1:0,null_bool:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],null_bool_:[],null_str:4,nullabl:3,nulladapt:[],number:[0,1,4,5,6,15],nvtzj26et1sp4qa3rctazoqgeidngo48wt:14,nxjsr1xs4jb:14,object:[0,1,3,4,5,6,7,14,15],object_:3,offici:9,ofo:[],often:[],older:[1,13],oldest:1,omit:15,omytkhrtmho8t33cgdytilcehaf5sj0:[],onc:[1,3,5],once:1,onli:[],only:[0,1,16],opengridengin:[],oper:[0,1,7],option:[0,1,3,15,16],optional:[0,3],optional_config:[3,15],order:[1,3,5,6,9,12,15],ordereddict:6,origin:[13,15],other:[0,9,13,14,16],otherwis:[1,5,6,16],our:[7,15],out:[0,3,10],outag:7,outdat:7,output:[0,1,3,4,7,12,15],output_:[],output_base_url:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],output_data_format:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],output_filename_pattern:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],output_lifetim:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],output_product:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],output_service_backend:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],outsid:15,over:12,overrid:[3,15],overwrit:[],overwritten:7,own:[0,1,3,5,6,9,13,16],packag:10,page:11,pair:9,parallel:0,param:[1,2,3,6,15],paramet:[0,1,3,4,5,6,9,15],parent:1,parm:[],pars:[0,3,4,7],parse_boolean_str:[],parser:3,part:[12,15,16],partial:1,particular:6,pass:[0,4,15],patch:14,path:[0,2,4,6,9,15],pattern:[0,1],payload:[6,14],pend:13,perform:[0,1,7],period:[0,7],perman:[0,15],persist:15,pgp:14,physic:[],pid:6,pip:10,place:[1,3,15],pleas:9,point:3,poll:5,poll_listen:5,popul:[1,3,5,6,15],port:[9,14],posit:[3,4,13],positive_int:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],possibl:3,post:[0,1,14,15],post_resourc:1,post_to_productstatu:1,postprocess:0,pre:[3,4],prepend:0,presed:[],present:[0,9,13],prevent:14,previou:[9,13],previous:5,primari:1,print:[4,15],print_and_mail_except:4,probabl:[14,15],proce:[3,10],process:[0,1,4,5,6,7,12,13,14,15],process_all_in_product_inst:5,process_data_inst:[0,5],process_health_check:[],process_job:5,process_next_ev:5,process_parti:[],process_rest_api:5,process_rpc_ev:[],produc:0,product:[0,1,12],product_inst:[],product_instance_uuid:5,productinst:[1,5],productstatu:[],productstatus_:[],productstatuslisten:9,productstatusresourceev:5,program:[5,6,10,13,15,16],proper:6,properti:[0,12],protect:3,protocol:13,provid:[0,1,3,6,12,14],publicli:14,pull:3,purpos:0,put:[14,15],pvqvp55:14,python3:[4,14],python:[0,4,9,10,12,14,15,16],qfb1tag:[],qsub:13,qu6dmvrtqhrry29wzxiig:14,queri:14,queu:13,queue:[5,6,7,12,13,14,15],quickli:[0,5],r5vhzqowijmv:14,radar:0,rais:[1,3,4,5,15],raw:6,reach:[0,5],reachabl:0,read:[],reason:15,receiv:[0,5,6,7,9,12,13],recent:7,recipi:9,recommend:10,reconstruct:6,recov:5,recurs:[3,9],refer:[1,3,5,6,7,9,16],referenc:9,reference_tim:0,reference_time_threshold:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],refus:16,regard:3,regardless:[13,15,16],registri:10,regular:[5,7],regularli:13,reimplement:3,reiniti:5,reinitialize_job:5,reject:[13,14],relat:[6,7],remov:[0,2,6],remove_item:6,remove_job:6,remove_required_uuid:1,render:[9,16],replac:3,replai:14,repond:14,report:[],report_event_queue_metr:5,report_job_status_metr:5,repositori:[0,15],repres:[1,4],represent:[3,6],request:[],requeu:0,requir:[0,1,3,9,15],require_productstatus_credenti:1,required_config:3,required_uuid:1,reschedul:0,reset:5,reset_event_queue_item_gener:5,resolv:[3,9],resolvabledepend:3,resolve_depend:3,resolved_config_sect:3,resourc:[0,1,4,5,7,13,15],resource_matches_hash_config:1,resource_matches_input_config:1,resourcetoooldexcept:[5,7],respons:[1,5],rest:[],rest_api_serv:5,rest_serv:[9,14],restart:[],restart_listen:5,restor:13,restore_queu:5,result:[0,5,7,13,15],retri:[0,1,4,5,15],retriev:[5,6,15],retry_backoff_factor:1,retry_interval_sec:1,retry_limit:1,retry_n:4,retryexcept:[7,15],reusabl:0,revers:[],right:15,rjp:[],rnyxal4ckovgk1:[],robust:12,root:16,rpc:[],rpcexception:7,rpcfailedexcept:7,rpcinvalidregexexcept:7,rpcwronginstanceidexcept:7,rtquie2outjhaixrf3a83qtetaump8cpjty31dgalztuzsy6isdzoxi:14,rtype:[],run:[],runtimeerror:[3,4,6],rxy74nyf0ntvuxjmwotr2c4xwn2mbkbqj10nnixwejrsm16gmjr6fckzdf9syv:[],s1we58xh:14,sake:15,same:[0,6],saniti:1,save:[5,15],scan:9,schedul:[1,5,12,15],scp:0,script:[0,2,15],sd0fczwg6rukvqtr8vjpnxhac946aml:[],search:[5,11],second:[0,1,5,14,15],secret_configuration:3,section:[],section_kei:3,see:[0,1,3,9,13,15],self:[1,15],selv:15,send:[4,5],send_email:[],sens:[7,12],sensit:14,sent:[4,7,14,15],separ:[0,3,4,6],sequenti:5,serial:[5,6,13],serv:0,server:[0,5,9,13,14],servic:[0,1,7],servicebackend:0,set:[],set_adapt:6,set_config_id:3,set_drain:5,set_health_check_heartbeat_interv:5,set_health_check_heartbeat_timeout:5,set_health_check_skip_heartbeat:5,set_health_check_timestamp:5,set_message_timestamp_threshold:5,set_no_drain:5,setup_process_parti:[],setup_reference_time_threshold:[],sever:[],sha256:15,sha256sum:15,shall:3,shell:[],shelladapt:[],ship:14,should:[0,1,2,5,6,7,9,12,15,16],show:15,shut:5,shutdown:[5,13],shutdownexcept:7,sigint:7,signal:[7,14],signatur:14,signature:14,sigterm:7,similar:[0,9],simplic:15,simplifi:14,singl:[0,1,3,4,5,7],size:[0,4],skeleton:15,skip:5,sleep:0,small:12,smooth:0,smtp_host:9,snap:0,snip:[],softwar:0,some:13,someth:15,soon:[3,6],sort:[],sourc:[0,1,2,3,4,5,6,7,10,15],special:[0,16],specif:[0,5,6,12,13],specifi:[0,1,2,3,9,15,16],split:[3,4],split_comma_separ:4,src:[],ssh:[0,9],ssh_host:[0,9],ssh_key_fil:[0,9],ssh_user:[0,9],standard:[12,15],start:[0,1,2,4,9,13,14,15,16],startup:[9,13],state:[2,13],statsd:[5,9,13],statu:[2,5,6,13,14],status_count:6,stdout:15,step:0,stop:5,storag:[6,12],store:[1,6,7,13,15],store_item:6,store_list:6,store_serialized_data:6,str:[3,4,5,6],strftime_iso8601:4,string:[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],strip:15,structur:[6,9,12],style:[],subclass:[1,3],subject:[],submit:13,subsequ:0,successfulli:5,sudo:10,suffici:1,suffix:[],suitabl:[0,4,6],sun:[],support:[0,14,15],sure:[5,12],surfac:0,sync:2,syntax:[],system:[0,10,12,13,14,15],t9w4loznmlqwlqruoki5yfwrx6n:[],tag:13,take:[0,1,5,15],task:[0,1,12],tbd:[],tell:5,templat:[0,1],temporari:2,terabyt:4,termin:15,test:[],test_executor:[],testexecutoradapt:[],text:[],than:[0,1,3,13,16],thei:[1,3,6,9,12,15],them:[3,5],themselv:1,thi:[0,1,3,5,6,7,9,10,14,15,16],thing:15,thoma:0,those:[],though:0,thread:0,thredd:[],thredds:0,thredds_base_url:0,thredds_poll_interv:0,thredds_poll_retri:0,threddsadapt:[],three:[12,15],threshold:[0,13],through:[0,1,5,14],thrown:[6,7],thu:[6,15],time:[0,1,4,5,7,13,14,15],time_str:4,timedelta:0,timeout:5,timer:5,timestamp:[4,5,13],timezon:4,tjygv3lcur9:[],toler:4,too:[5,6,7],took:13,tool:[0,14],top:9,total:6,touch:6,translat:[],transport:12,treat:7,trigger:[0,12],tupl:[3,4],turn:0,tutori:[],two:[0,3,9,15],txt:0,type:[0,1,3,4,5,13,14,15],typic:7,ubmo3i1zsiwttghtabcybvfv8omyz1xkr9hyor5gtdrgz8qhuns26hznwy7cd:[],ubqrdm:[],ultim:12,under:[0,6,7,16],underli:7,unexpect:7,unfinish:5,unicod:3,uniqu:9,unseri:6,unset:1,until:[],unv34bsgr:[],updat:[13,15],upload:10,upon:[9,13,15],uqsscfx14mkfq:[],uri:5,url:[0,1,4,9,15],url_to_filenam:[4,15],usabl:15,usage:3,use:[6,16],used:6,user:[0,1,9,14,15],userland:13,usernam:0,usr:4,usual:6,utc:4,utf:14,util:[0,12],uuid:[0,1,5],v6rt5fyjuvjgxlin7:[],valid:[1,5,7,15],validate_resourc:1,valu:[0,1,3,4,9,15],valueerror:4,variabl:[],variable:1,variou:7,verifi:[0,15],version:[5,13,14],via:[0,5],virtual:0,virtualenv:10,wai:13,wait:[0,1,4,13],want:[0,4,6,10,15],warn:4,warning:[4,15],wehyzq3x7xvh81cflk7:[],well:[4,6],were:[6,13],what:[],when:[0,3,4,5,6,7,9,14,15,16],where:[1,4,9],whether:[1,3,15],which:[0,1,3,5,6,14,15],whitespac:9,who:15,whose:1,wide:10,wire:[6,9,15],within:[14,15],work:1,wrapper:6,write:[],write_missing_radar:0,written:0,wsgiserver:14,x8hljhs9:14,x9su799iqakxujmvsbllq18my0f1cwior:[],x9su7lumqak:14,xn97zj7prn4adom88rxwwkumtrmfxi9crjb5s0rtnjoiwtjsrkmur3m:14,xu7wwwyqfub:14,y9xgay:[],yes:1,yet:0,yield:7,ylecaqy2c5zorviuqlr:14,you:[0,1,3,6,9,10,14,15,16],your:[0,3,10,15,16],yubjhvequcxqe5:[],zero:[1,4,6],zk_get_seri:6,zk_get_str:6,zk_immediate_store_dis:6,zk_immediate_store_en:6,zone:[],zookeep:[4,5,6,7,9,13],zookeeper_group_id:4,zookeeperdatatoolargeexcept:[6,7],zookeepererror:6,zzr:14},titles:["Adapter documentation","eva.base.adapter","eva.base.executor","eva.config","eva","eva.eventloop","eva.eventqueue","eva.exceptions","API documentation","Configuration","Development","EVA: the Event Adapter","Introduction to Event Adapter","Metrics","HTTP REST API","Tutorial","Configuration directives"],titleterms:{"abstract":16,"class":16,"function":[],adapt:[0,1,15],adapter:[0,11,12],adding:15,adpter:[],all:1,anatomi:12,api:[8,14],automat:15,base:[1,2],check:14,checksumverificationadapt:0,client:14,code:15,config:3,configur:[1,9,15,16],contain:10,content:[],creat:15,cwfadapter:0,deleteadapt:0,develop:10,direct:16,distributionadapt:0,docker:10,document:[0,8,10],environ:10,eva:[1,2,3,4,5,6,7,11,12],event:[11,12],eventloop:5,eventqueu:6,exampleadapt:0,except:7,exception:[],execut:15,executor:2,file:9,fimexadapt:0,fimexfillfileadapt:0,fimexgrib2netcdfadapt:0,format:9,gener:10,gridppadapt:0,health:14,http:14,includ:16,indice:11,introduct:12,job:15,lint:10,list:0,load:15,metadata:15,metric:13,modul:[],nowcastppadapt:0,nulladapt:0,onli:14,productstatu:15,read:14,report:[],request:14,rest:14,restart:15,rpc:14,run:10,section:16,set:10,shell:15,tabl:11,test:10,testexecutoradapt:0,threddsadapt:0,tutori:15,variabl:15,welcom:[],write:[14,15]}})