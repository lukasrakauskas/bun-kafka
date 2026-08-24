// bun-kafka napi backend — thin wrappers around librdkafka
#include <node_api.h>
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define OK(env, s) do { if ((s) != napi_ok) return NULL; } while (0)

static napi_value u32_val(napi_env env, uint32_t v) {
  napi_value n; napi_create_uint32(env, v, &n); return n;
}
static napi_value i32_val(napi_env env, int32_t v) {
  napi_value n; napi_create_int32(env, v, &n); return n;
}
static napi_value i64_val(napi_env env, int64_t v) {
  napi_value n; napi_create_bigint_int64(env, v, &n); return n;
}
static napi_value str_val(napi_env env, const char *s) {
  napi_value n; napi_create_string_utf8(env, s ? s : "", NAPI_AUTO_LENGTH, &n); return n;
}
static napi_value throw_err(napi_env env, int code, const char *msg) {
  char buf[512];
  snprintf(buf, sizeof(buf), "%d:%s", code, msg ? msg : rd_kafka_err2str(code));
  napi_throw_error(env, NULL, buf);
  return NULL;
}
static char *req_str(napi_env env, napi_value v, char *buf, size_t n) {
  size_t len = 0;
  if (napi_get_value_string_utf8(env, v, buf, n, &len) != napi_ok) return NULL;
  return buf;
}
static int req_i32(napi_env env, napi_value v) {
  int32_t x = 0; napi_get_value_int32(env, v, &x); return x;
}
static int64_t req_i64(napi_env env, napi_value v) {
  napi_valuetype t; napi_typeof(env, v, &t);
  if (t == napi_bigint) { int64_t x=0; bool lossless=true; napi_get_value_bigint_int64(env,v,&x,&lossless); return x; }
  double d=0; napi_get_value_double(env,v,&d); return (int64_t)d;
}

typedef struct { rd_kafka_t *rk; } Handle;
typedef struct { rd_kafka_message_t *msg; int freed; } MsgWrap;

static void handle_finalizer(napi_env env, void *data, void *hint) {
  (void)env; (void)hint;
  Handle *h = data;
  if (h && h->rk) { rd_kafka_destroy(h->rk); h->rk = NULL; }
  free(h);
}
static void msg_finalizer(napi_env env, void *data, void *hint) {
  (void)env; (void)hint;
  MsgWrap *w = data;
  if (w && w->msg && !w->freed) { rd_kafka_message_destroy(w->msg); w->freed = 1; }
  free(w);
}

static rd_kafka_conf_t *conf_from_obj(napi_env env, napi_value obj, char *errstr, size_t errlen) {
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  napi_value names;
  uint32_t len = 0;
  if (napi_get_property_names(env, obj, &names) != napi_ok) return conf;
  napi_get_array_length(env, names, &len);
  for (uint32_t i = 0; i < len; i++) {
    napi_value k, v;
    napi_get_element(env, names, i, &k);
    napi_get_property(env, obj, k, &v);
    char key[256], val[1024];
    req_str(env, k, key, sizeof(key));
    napi_valuetype t; napi_typeof(env, v, &t);
    if (t == napi_boolean) {
      bool b=false; napi_get_value_bool(env,v,&b);
      snprintf(val,sizeof(val),"%s", b?"true":"false");
    } else if (t == napi_number) {
      double d=0; napi_get_value_double(env,v,&d);
      if (d == (int64_t)d) snprintf(val,sizeof(val),"%lld",(long long)(int64_t)d);
      else snprintf(val,sizeof(val),"%g",d);
    } else {
      req_str(env, v, val, sizeof(val));
    }
    if (rd_kafka_conf_set(conf, key, val, errstr, errlen) != RD_KAFKA_CONF_OK) {
      rd_kafka_conf_destroy(conf);
      return NULL;
    }
  }
  return conf;
}

static napi_value make_handle(napi_env env, rd_kafka_t *rk) {
  Handle *h = calloc(1, sizeof(*h));
  h->rk = rk;
  napi_value ext;
  napi_create_external(env, h, handle_finalizer, NULL, &ext);
  return ext;
}
static Handle *get_handle(napi_env env, napi_value v) {
  void *p = NULL; napi_get_value_external(env, v, &p); return (Handle *)p;
}

static napi_value version(napi_env env, napi_callback_info info) {
  (void)info;
  napi_value obj, n, s;
  napi_create_object(env, &obj);
  napi_create_int32(env, rd_kafka_version(), &n);
  napi_set_named_property(env, obj, "number", n);
  napi_create_string_utf8(env, rd_kafka_version_str(), NAPI_AUTO_LENGTH, &s);
  napi_set_named_property(env, obj, "string", s);
  return obj;
}
static napi_value err2str_js(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  return str_val(env, rd_kafka_err2str(req_i32(env, args[0])));
}

static napi_value producer_new(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  char errstr[512];
  rd_kafka_conf_t *conf = conf_from_obj(env, args[0], errstr, sizeof(errstr));
  if (!conf) return throw_err(env, -1, errstr);
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (!rk) return throw_err(env, -1, errstr);
  return make_handle(env, rk);
}
static napi_value consumer_new(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  char errstr[512];
  rd_kafka_conf_t *conf = conf_from_obj(env, args[0], errstr, sizeof(errstr));
  if (!conf) return throw_err(env, -1, errstr);
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (!rk) return throw_err(env, -1, errstr);
  rd_kafka_poll_set_consumer(rk);
  return make_handle(env, rk);
}
static napi_value handle_close(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  int is_consumer = argc>1 ? req_i32(env, args[1]) : 0;
  if (h && h->rk) {
    if (is_consumer) rd_kafka_consumer_close(h->rk);
    rd_kafka_destroy(h->rk);
    h->rk = NULL;
  }
  return NULL;
}

static void bytes_from_js(napi_env env, napi_value v, void **ptr, size_t *len, void **alloc) {
  *ptr=NULL; *len=0; *alloc=NULL;
  if (v == NULL) return;
  napi_valuetype t; napi_typeof(env, v, &t);
  if (t == napi_null || t == napi_undefined) return;
  bool is_buf=false; napi_is_buffer(env, v, &is_buf);
  if (is_buf) { napi_get_buffer_info(env, v, ptr, len); return; }
  bool is_ta=false; napi_is_typedarray(env, v, &is_ta);
  if (is_ta) {
    napi_typedarray_type tt; size_t l=0; void *data=NULL; napi_value ab; size_t off=0;
    napi_get_typedarray_info(env, v, &tt, &l, &data, &ab, &off);
    *ptr = data; *len = l; return;
  }
  if (t == napi_string) {
    size_t n=0; napi_get_value_string_utf8(env, v, NULL, 0, &n);
    char *s = malloc(n+1);
    napi_get_value_string_utf8(env, v, s, n+1, &n);
    *ptr=s; *len=n; *alloc=s;
  }
}

static napi_value producer_send(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  napi_value msg = args[1];
  napi_value v;
  char topic[512];
  napi_get_named_property(env, msg, "topic", &v);
  req_str(env, v, topic, sizeof(topic));

  void *val=NULL,*key=NULL,*va=NULL,*ka=NULL; size_t vlen=0,klen=0;
  if (napi_get_named_property(env, msg, "value", &v)==napi_ok) bytes_from_js(env,v,&val,&vlen,&va);
  if (napi_get_named_property(env, msg, "key", &v)==napi_ok) bytes_from_js(env,v,&key,&klen,&ka);

  int32_t partition = RD_KAFKA_PARTITION_UA;
  if (napi_get_named_property(env, msg, "partition", &v)==napi_ok) {
    napi_valuetype t; napi_typeof(env,v,&t);
    if (t == napi_number) partition = req_i32(env,v);
  }
  int64_t timestamp = 0; int has_ts=0;
  if (napi_get_named_property(env, msg, "timestamp", &v)==napi_ok) {
    napi_valuetype t; napi_typeof(env,v,&t);
    if (t != napi_undefined && t != napi_null) { timestamp = req_i64(env,v); has_ts=1; }
  }

  rd_kafka_headers_t *hdrs = NULL;
  if (napi_get_named_property(env, msg, "headers", &v)==napi_ok) {
    napi_valuetype t; napi_typeof(env,v,&t);
    if (t == napi_object) {
      napi_value names; uint32_t n=0;
      napi_get_property_names(env, v, &names);
      napi_get_array_length(env, names, &n);
      if (n) {
        hdrs = rd_kafka_headers_new(n);
        for (uint32_t i=0;i<n;i++) {
          napi_value hk, hv; char hname[256];
          napi_get_element(env, names, i, &hk);
          napi_get_property(env, v, hk, &hv);
          req_str(env, hk, hname, sizeof(hname));
          void *hp=NULL,*ha=NULL; size_t hl=0;
          bytes_from_js(env, hv, &hp, &hl, &ha);
          rd_kafka_header_add(hdrs, hname, -1, hp, hp? (ssize_t)hl : 0);
          free(ha);
        }
      }
    }
  }

  rd_kafka_vu_t vus[8];
  size_t n=0;
  vus[n++] = (rd_kafka_vu_t){ .vtype = RD_KAFKA_VTYPE_TOPIC, .u.cstr = topic };
  if (partition != RD_KAFKA_PARTITION_UA)
    vus[n++] = (rd_kafka_vu_t){ .vtype = RD_KAFKA_VTYPE_PARTITION, .u.i32 = partition };
  if (val) vus[n++] = (rd_kafka_vu_t){ .vtype = RD_KAFKA_VTYPE_VALUE, .u.mem = { .ptr = val, .size = vlen } };
  if (key) vus[n++] = (rd_kafka_vu_t){ .vtype = RD_KAFKA_VTYPE_KEY, .u.mem = { .ptr = key, .size = klen } };
  vus[n++] = (rd_kafka_vu_t){ .vtype = RD_KAFKA_VTYPE_MSGFLAGS, .u.i = RD_KAFKA_MSG_F_COPY };
  if (has_ts) vus[n++] = (rd_kafka_vu_t){ .vtype = RD_KAFKA_VTYPE_TIMESTAMP, .u.i64 = timestamp };
  if (hdrs) vus[n++] = (rd_kafka_vu_t){ .vtype = RD_KAFKA_VTYPE_HEADERS, .u.headers = hdrs };
  /* produceva: do not pass VTYPE_END; cnt is number of real entries */

  rd_kafka_error_t *err = rd_kafka_produceva(h->rk, vus, n);
  free(va); free(ka);
  if (err) {
    if (hdrs) rd_kafka_headers_destroy(hdrs);
    int code = rd_kafka_error_code(err);
    const char *es = rd_kafka_error_string(err);
    rd_kafka_error_destroy(err);
    return throw_err(env, code, es);
  }
  return NULL;
}

static napi_value producer_flush(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  int r = rd_kafka_flush(h->rk, req_i32(env, args[1]));
  if (r) return throw_err(env, r, NULL);
  return NULL;
}
static napi_value producer_poll(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  return i32_val(env, rd_kafka_poll(h->rk, req_i32(env, args[1])));
}
static napi_value producer_outq(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  return i32_val(env, rd_kafka_outq_len(h->rk));
}

static rd_kafka_topic_partition_list_t *list_from_js(napi_env env, napi_value arr) {
  uint32_t n=0; napi_get_array_length(env, arr, &n);
  rd_kafka_topic_partition_list_t *list = rd_kafka_topic_partition_list_new((int)n);
  for (uint32_t i=0;i<n;i++) {
    napi_value el, v; char topic[512];
    napi_get_element(env, arr, i, &el);
    napi_valuetype t; napi_typeof(env, el, &t);
    if (t == napi_string) {
      req_str(env, el, topic, sizeof(topic));
      rd_kafka_topic_partition_list_add(list, topic, -1);
    } else {
      int32_t part = -1; int64_t off = RD_KAFKA_OFFSET_INVALID; int has_off=0;
      napi_get_named_property(env, el, "topic", &v); req_str(env, v, topic, sizeof(topic));
      if (napi_get_named_property(env, el, "partition", &v)==napi_ok) part = req_i32(env,v);
      if (napi_get_named_property(env, el, "offset", &v)==napi_ok) {
        napi_valuetype ot; napi_typeof(env,v,&ot);
        if (ot != napi_undefined && ot != napi_null) { off = req_i64(env,v); has_off=1; }
      }
      rd_kafka_topic_partition_t *tp = rd_kafka_topic_partition_list_add(list, topic, part);
      if (has_off) tp->offset = off;
    }
  }
  return list;
}
static napi_value list_to_js(napi_env env, rd_kafka_topic_partition_list_t *list) {
  napi_value arr; napi_create_array_with_length(env, list ? list->cnt : 0, &arr);
  if (!list) return arr;
  for (int i=0;i<list->cnt;i++) {
    rd_kafka_topic_partition_t *tp = &list->elems[i];
    napi_value o, v;
    napi_create_object(env, &o);
    napi_set_named_property(env, o, "topic", str_val(env, tp->topic));
    napi_set_named_property(env, o, "partition", i32_val(env, tp->partition));
    napi_set_named_property(env, o, "offset", i64_val(env, tp->offset));
    napi_set_named_property(env, o, "err", i32_val(env, tp->err));
    napi_set_element(env, arr, i, o);
  }
  return arr;
}

static napi_value consumer_subscribe(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  int r = rd_kafka_subscribe(h->rk, list);
  rd_kafka_topic_partition_list_destroy(list);
  if (r) return throw_err(env, r, NULL);
  return NULL;
}
static napi_value consumer_unsubscribe(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  int r = rd_kafka_unsubscribe(h->rk);
  if (r) return throw_err(env, r, NULL);
  return NULL;
}
static napi_value consumer_subscription(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list=NULL;
  int r = rd_kafka_subscription(h->rk, &list);
  if (r) return throw_err(env, r, NULL);
  napi_value out = list_to_js(env, list);
  rd_kafka_topic_partition_list_destroy(list);
  return out;
}
static napi_value consumer_assign(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  uint32_t n=0; napi_get_array_length(env, args[1], &n);
  if (!n) {
    int r = rd_kafka_assign(h->rk, NULL);
    if (r) return throw_err(env, r, NULL);
    return NULL;
  }
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  int r = rd_kafka_assign(h->rk, list);
  rd_kafka_topic_partition_list_destroy(list);
  if (r) return throw_err(env, r, NULL);
  return NULL;
}
static napi_value consumer_assignment(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list=NULL;
  int r = rd_kafka_assignment(h->rk, &list);
  if (r) return throw_err(env, r, NULL);
  napi_value out = list_to_js(env, list);
  rd_kafka_topic_partition_list_destroy(list);
  return out;
}

static napi_value msg_to_js(napi_env env, rd_kafka_message_t *msg) {
  MsgWrap *w = calloc(1, sizeof(*w));
  w->msg = msg;
  napi_value ext; napi_create_external(env, w, msg_finalizer, NULL, &ext);

  napi_value obj; napi_create_object(env, &obj);
  const char *topic = msg->rkt ? rd_kafka_topic_name(msg->rkt) : "";
  napi_set_named_property(env, obj, "topic", str_val(env, topic));
  napi_set_named_property(env, obj, "partition", i32_val(env, msg->partition));
  napi_set_named_property(env, obj, "offset", i64_val(env, msg->offset));

  napi_value val, key;
  if (msg->payload && msg->len)
    napi_create_buffer_copy(env, msg->len, msg->payload, NULL, &val);
  else napi_get_null(env, &val);
  if (msg->key && msg->key_len)
    napi_create_buffer_copy(env, msg->key_len, msg->key, NULL, &key);
  else napi_get_null(env, &key);
  napi_set_named_property(env, obj, "value", val);
  napi_set_named_property(env, obj, "key", key);

  rd_kafka_timestamp_type_t tstype;
  int64_t ts = rd_kafka_message_timestamp(msg, &tstype);
  napi_set_named_property(env, obj, "timestamp", i64_val(env, ts));
  napi_set_named_property(env, obj, "timestampType", i32_val(env, (int)tstype));
  napi_set_named_property(env, obj, "brokerId", i32_val(env, rd_kafka_message_broker_id(msg)));

  napi_value headers; napi_create_object(env, &headers);
  rd_kafka_headers_t *hdrs=NULL;
  if (rd_kafka_message_headers(msg, &hdrs) == RD_KAFKA_RESP_ERR_NO_ERROR && hdrs) {
    const char *name; const void *hval; size_t hlen;
    for (size_t i=0; rd_kafka_header_get_all(hdrs, i, &name, &hval, &hlen)==0; i++) {
      napi_value hv;
      if (hval && hlen) napi_create_buffer_copy(env, hlen, hval, NULL, &hv);
      else napi_get_null(env, &hv);
      napi_set_named_property(env, headers, name, hv);
    }
  }
  napi_set_named_property(env, obj, "headers", headers);
  napi_set_named_property(env, obj, "_wrap", ext);
  return obj;
}

static napi_value message_done(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  void *p=NULL; napi_get_value_external(env, args[0], &p);
  MsgWrap *w = p;
  if (w && w->msg && !w->freed) { rd_kafka_message_destroy(w->msg); w->msg=NULL; w->freed=1; }
  return NULL;
}

static napi_value consumer_poll(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_message_t *msg = rd_kafka_consumer_poll(h->rk, req_i32(env, args[1]));
  if (!msg) { napi_value n; napi_get_null(env,&n); return n; }
  if (msg->err == RD_KAFKA_RESP_ERR__TIMED_OUT || msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
    rd_kafka_message_destroy(msg);
    napi_value n; napi_get_null(env,&n); return n;
  }
  if (msg->err) {
    const char *es = rd_kafka_message_errstr(msg);
    int code = msg->err;
    rd_kafka_message_destroy(msg);
    return throw_err(env, code, es);
  }
  return msg_to_js(env, msg);
}

static napi_value consumer_commit(napi_env env, napi_callback_info info) {
  size_t argc=3; napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  int async = req_i32(env, args[2]);
  napi_valuetype t; napi_typeof(env, args[1], &t);
  if (t == napi_null || t == napi_undefined) {
    int r = rd_kafka_commit(h->rk, NULL, async);
    if (r) return throw_err(env, r, NULL);
    return NULL;
  }
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  int r = rd_kafka_commit(h->rk, list, async);
  rd_kafka_topic_partition_list_destroy(list);
  if (r) return throw_err(env, r, NULL);
  return NULL;
}
static napi_value consumer_committed(napi_env env, napi_callback_info info) {
  size_t argc=3; napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  int r = rd_kafka_committed(h->rk, list, req_i32(env, args[2]));
  if (r) { rd_kafka_topic_partition_list_destroy(list); return throw_err(env, r, NULL); }
  napi_value out = list_to_js(env, list);
  rd_kafka_topic_partition_list_destroy(list);
  return out;
}
static napi_value consumer_position(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  int r = rd_kafka_position(h->rk, list);
  if (r) { rd_kafka_topic_partition_list_destroy(list); return throw_err(env, r, NULL); }
  napi_value out = list_to_js(env, list);
  rd_kafka_topic_partition_list_destroy(list);
  return out;
}
static napi_value consumer_seek(napi_env env, napi_callback_info info) {
  size_t argc=3; napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  rd_kafka_error_t *err = rd_kafka_seek_partitions(h->rk, list, req_i32(env, args[2]));
  rd_kafka_topic_partition_list_destroy(list);
  if (err) {
    int code = rd_kafka_error_code(err);
    const char *es = rd_kafka_error_string(err);
    rd_kafka_error_destroy(err);
    return throw_err(env, code, es);
  }
  return NULL;
}
static napi_value consumer_pause(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  int r = rd_kafka_pause_partitions(h->rk, list);
  rd_kafka_topic_partition_list_destroy(list);
  if (r) return throw_err(env, r, NULL);
  return NULL;
}
static napi_value consumer_resume(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  int r = rd_kafka_resume_partitions(h->rk, list);
  rd_kafka_topic_partition_list_destroy(list);
  if (r) return throw_err(env, r, NULL);
  return NULL;
}
static napi_value consumer_store(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  int r = rd_kafka_offsets_store(h->rk, list);
  rd_kafka_topic_partition_list_destroy(list);
  if (r) return throw_err(env, r, NULL);
  return NULL;
}
static napi_value query_wm(napi_env env, napi_callback_info info) {
  size_t argc=4; napi_value args[4];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  char topic[512]; req_str(env, args[1], topic, sizeof(topic));
  int64_t low=0, high=0;
  int r = rd_kafka_query_watermark_offsets(h->rk, topic, req_i32(env,args[2]), &low, &high, req_i32(env,args[3]));
  if (r) return throw_err(env, r, NULL);
  napi_value o; napi_create_object(env,&o);
  napi_set_named_property(env,o,"low", i64_val(env,low));
  napi_set_named_property(env,o,"high", i64_val(env,high));
  return o;
}
static napi_value get_wm(napi_env env, napi_callback_info info) {
  size_t argc=3; napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  char topic[512]; req_str(env, args[1], topic, sizeof(topic));
  int64_t low=0, high=0;
  int r = rd_kafka_get_watermark_offsets(h->rk, topic, req_i32(env,args[2]), &low, &high);
  if (r) return throw_err(env, r, NULL);
  napi_value o; napi_create_object(env,&o);
  napi_set_named_property(env,o,"low", i64_val(env,low));
  napi_set_named_property(env,o,"high", i64_val(env,high));
  return o;
}
static napi_value offsets_for_times(napi_env env, napi_callback_info info) {
  size_t argc=3; napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  rd_kafka_topic_partition_list_t *list = list_from_js(env, args[1]);
  int r = rd_kafka_offsets_for_times(h->rk, list, req_i32(env, args[2]));
  if (r) { rd_kafka_topic_partition_list_destroy(list); return throw_err(env, r, NULL); }
  napi_value out = list_to_js(env, list);
  rd_kafka_topic_partition_list_destroy(list);
  return out;
}
static napi_value member_id(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  char *id = rd_kafka_memberid(h->rk);
  if (!id) { napi_value n; napi_get_null(env,&n); return n; }
  napi_value s = str_val(env, id);
  rd_kafka_mem_free(h->rk, id);
  return s;
}
static napi_value assignment_lost(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  return i32_val(env, rd_kafka_assignment_lost(h->rk));
}
static napi_value rebalance_protocol(napi_env env, napi_callback_info info) {
  size_t argc=1; napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  return str_val(env, rd_kafka_rebalance_protocol(h->rk));
}

static napi_value admin_metadata(napi_env env, napi_callback_info info) {
  size_t argc=3; napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  const rd_kafka_metadata_t *meta=NULL;
  int r = rd_kafka_metadata(h->rk, req_i32(env,args[1]), NULL, &meta, req_i32(env,args[2]));
  if (r) return throw_err(env, r, NULL);
  napi_value out, brokers, topics;
  napi_create_object(env, &out);
  napi_create_array_with_length(env, meta->broker_cnt, &brokers);
  for (int i=0;i<meta->broker_cnt;i++) {
    napi_value b; napi_create_object(env,&b);
    napi_set_named_property(env,b,"id", i32_val(env, meta->brokers[i].id));
    napi_set_named_property(env,b,"host", str_val(env, meta->brokers[i].host));
    napi_set_named_property(env,b,"port", i32_val(env, meta->brokers[i].port));
    napi_set_element(env, brokers, i, b);
  }
  napi_create_array_with_length(env, meta->topic_cnt, &topics);
  for (int i=0;i<meta->topic_cnt;i++) {
    napi_value t, parts; napi_create_object(env,&t);
    napi_set_named_property(env,t,"name", str_val(env, meta->topics[i].topic));
    napi_set_named_property(env,t,"err", i32_val(env, meta->topics[i].err));
    napi_create_array_with_length(env, meta->topics[i].partition_cnt, &parts);
    for (int j=0;j<meta->topics[i].partition_cnt;j++) {
      napi_value p; napi_create_object(env,&p);
      napi_set_named_property(env,p,"id", i32_val(env, meta->topics[i].partitions[j].id));
      napi_set_named_property(env,p,"err", i32_val(env, meta->topics[i].partitions[j].err));
      napi_set_named_property(env,p,"leader", i32_val(env, meta->topics[i].partitions[j].leader));
      napi_set_element(env, parts, j, p);
    }
    napi_set_named_property(env,t,"partitions", parts);
    napi_set_element(env, topics, i, t);
  }
  napi_set_named_property(env, out, "brokers", brokers);
  napi_set_named_property(env, out, "topics", topics);
  rd_kafka_metadata_destroy(meta);
  return out;
}
static napi_value admin_clusterid(napi_env env, napi_callback_info info) {
  size_t argc=2; napi_value args[2];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  Handle *h = get_handle(env, args[0]);
  char *id = rd_kafka_clusterid(h->rk, req_i32(env, args[1]));
  if (!id) { napi_value n; napi_get_null(env,&n); return n; }
  napi_value s = str_val(env, id);
  rd_kafka_mem_free(h->rk, id);
  return s;
}

#define EXPORT(name, fn) do {   napi_value f; napi_create_function(env, name, NAPI_AUTO_LENGTH, fn, NULL, &f);   napi_set_named_property(env, exports, name, f); } while(0)

static napi_value init(napi_env env, napi_value exports) {
  EXPORT("version", version);
  EXPORT("err2str", err2str_js);
  EXPORT("producerNew", producer_new);
  EXPORT("consumerNew", consumer_new);
  EXPORT("handleClose", handle_close);
  EXPORT("producerSend", producer_send);
  EXPORT("producerFlush", producer_flush);
  EXPORT("producerPoll", producer_poll);
  EXPORT("producerOutq", producer_outq);
  EXPORT("consumerSubscribe", consumer_subscribe);
  EXPORT("consumerUnsubscribe", consumer_unsubscribe);
  EXPORT("consumerSubscription", consumer_subscription);
  EXPORT("consumerAssign", consumer_assign);
  EXPORT("consumerAssignment", consumer_assignment);
  EXPORT("consumerPoll", consumer_poll);
  EXPORT("consumerCommit", consumer_commit);
  EXPORT("consumerCommitted", consumer_committed);
  EXPORT("consumerPosition", consumer_position);
  EXPORT("consumerSeek", consumer_seek);
  EXPORT("consumerPause", consumer_pause);
  EXPORT("consumerResume", consumer_resume);
  EXPORT("consumerStore", consumer_store);
  EXPORT("queryWatermarkOffsets", query_wm);
  EXPORT("getWatermarkOffsets", get_wm);
  EXPORT("offsetsForTimes", offsets_for_times);
  EXPORT("memberId", member_id);
  EXPORT("assignmentLost", assignment_lost);
  EXPORT("rebalanceProtocol", rebalance_protocol);
  EXPORT("messageDone", message_done);
  EXPORT("adminMetadata", admin_metadata);
  EXPORT("adminClusterId", admin_clusterid);
  return exports;
}

NAPI_MODULE(bun_kafka_native, init)
