#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <ctype.h>
#include <pthread.h>
#include <poll.h>

#include "vendor_bbfd.h"
#include "vendor_defs.h"
#include "usp_api.h"
#include "os_utils.h"
#include "common_defs.h"
#include "data_model.h"
#include "str_vector.h"
#include "text_utils.h"
#include "strncpyt.h"

#include <libubus.h>
#include <libubox/blobmsg.h>
#include <libubox/blobmsg_json.h>
#include <libubox/uloop.h>


#define USP_PROTO			"usp"
#define BBFD_TIMEOUT_SYNC		60000
#define DEVICE_ROOT			"Device."
#define BBFD_UBUS			"bbfd.raw"
#define BBFD_OPERATE_ARGS_MAX		32
#define VALUE_CHANGE_POLL_PERIOD_MS	(VALUE_CHANGE_POLL_PERIOD * 1000)

#ifndef __unused
#define __unused __attribute__((unused))
#endif

enum bbfd_thread_cmd {
	BBFD_THD_CMD_EXIT,
	BBFD_THD_CMD_WAKEUP
};

enum bbfd_cmd_type {
	/* Commands forwared to bbfd via ubus */
	BBFD_CMD_UBUS_ASYNC,
	BBFD_CMD_UBUS_SYNC,

	/* Locally implemented commands */
	BBFD_CMD_VENDOR_BASE,
	BBFD_CMD_SCHEDULE_TIMER
};

struct cmd_operate_params {
	kv_vector_t args;
};

struct bbfd_cmd_ubus {
	const char *cmd;
	struct blob_attr *args;
	struct blob_attr *resp;
	struct ubus_request req;
};

struct schedule_timer_args {
	unsigned delay_sec;
};

struct bbfd_cmd_vendor {
	union {
		struct schedule_timer_args sched_timer;
	};
	union {
		struct uloop_timeout timo;
	};
	char path[MAX_DM_PATH];
};

struct bbfd_cmd {
	enum bbfd_cmd_type type;
	int inst;
	int err;

	union {
		struct bbfd_cmd_ubus ubus;
		struct bbfd_cmd_vendor vendor;
	};

	struct list_head list;

	void (*complete_cb)(struct bbfd_cmd *cmd, struct blob_attr *data);
};

struct bbfd_get_result {
	int fault;
	char *buf;
	int buflen;
};

struct bbfd_add_result {
	int fault;
	int instance;
};

struct bbfd_operate_result {
	int fault;
	kv_vector_t *out;
};

extern bool is_running_cli_local_command;

static struct ubus_context *ubus_ctx;
static struct blob_buf gb;
static uint32_t bbfd_ubus_id = 0;

static LIST_HEAD(cmd_queue);
static pthread_mutex_t cmd_queue_mutex;

static str_vector_t g_instances;
static int cmd_pipe[2] = { -1, -1 };
static int wait_pipe[2] = { -1, -1 };

static void bbfd_thd_exec_async_cmd(struct bbfd_cmd *cmd);
static void bbfd_thd_exec_sync_cmd(struct bbfd_cmd *cmd);
static void bbfd_thd_schedule_timer(struct bbfd_cmd *cmd);

static void monitor_instances_cb(struct uloop_timeout *t);

static struct uloop_timeout monitor_instances_timer = {
	.cb = monitor_instances_cb
};


static struct blob_attr * get_parameters(struct blob_attr *msg)
{
	struct blob_attr *params = NULL;
	struct blob_attr *cur;
	int rem;

	blobmsg_for_each_attr(cur, msg, rem) {
		if (blobmsg_type(cur) == BLOBMSG_TYPE_ARRAY) {
			params = cur;
			break;
		}
	}
	return params;
}

static void bbfd_get_param_init(struct blob_buf *b, const char *path, bool usp_only)
{
	void *a;

	blob_buf_init(b, 0);
	a = blobmsg_open_array(b, "paths");
	blobmsg_add_string(b, NULL, path);
	blobmsg_close_array(b, a);

	if (usp_only)
		blobmsg_add_string(b, "proto", USP_PROTO);
}

static void bbfd_param_init(struct blob_buf *b, const char *path)
{
	blob_buf_init(b, 0);
	blobmsg_add_string(b, "path", path);
	blobmsg_add_string(b, "proto", USP_PROTO);
}

static void bbfd_operate_param_init(struct blob_buf *b,
				const char *path,
				const kv_vector_t *args)
{
	bbfd_param_init(b, path);

	if (args) {
		void *t = blobmsg_open_table(b, "params");
		int i;

		for (i = 0; i < args->num_entries; i++) {
			const kv_pair_t *kv = &args->vector[i];

			blobmsg_add_string(b, kv->key, kv->value);
		}
		blobmsg_close_table(b, t);
	}
}

static int pipe_init(int p[2])
{
	if (pipe2(p, O_NONBLOCK) == -1) {
		USP_LOG_Error("Create pipe() failed: %s", strerror(errno));
		return -1;
	}
	return 0;
}

static void pipe_deinit(int p[2])
{
	close(p[0]);
	close(p[1]);
}

static void bbfd_thread_cmd(enum bbfd_thread_cmd cmd)
{
	const int c = cmd;

	write(cmd_pipe[1], &c, sizeof(c));
}

static void wait_pipe_wakeup()
{
	const int cmd = 0;

	write(wait_pipe[1], &cmd, sizeof(cmd));
}

static void wait_pipe_wait()
{
	struct pollfd pfd = {
		.fd = wait_pipe[0],
		.events = POLLIN | POLLERR,
	};
	int cmd;

	poll(&pfd, 1, BBFD_TIMEOUT_SYNC);
	read(wait_pipe[0], &cmd, sizeof(cmd));
}

static void bbfd_thread_process_cmd(struct bbfd_cmd *cmd)
{
	switch (cmd->type) {
	case BBFD_CMD_UBUS_ASYNC:
		bbfd_thd_exec_async_cmd(cmd);
		break;
	case BBFD_CMD_UBUS_SYNC:
		bbfd_thd_exec_sync_cmd(cmd);
		break;
	case BBFD_CMD_SCHEDULE_TIMER:
		bbfd_thd_schedule_timer(cmd);
		break;
	default:
		break;
	}
}

static struct bbfd_cmd * bbfd_cmd_new()
{
	struct bbfd_cmd *cmd = USP_MALLOC(sizeof(*cmd));

	/* NULL check is done in USP_MALLOC */
	memset(cmd, 0, sizeof(*cmd));
	cmd->inst = INVALID;

	return cmd;
}

static void bbfd_cmd_free(struct bbfd_cmd *cmd)
{
	if (cmd->list.next) {
		pthread_mutex_lock(&cmd_queue_mutex);
		list_del(&cmd->list);
		pthread_mutex_unlock(&cmd_queue_mutex);
	}

	if (cmd->type < BBFD_CMD_VENDOR_BASE) {
		free(cmd->ubus.resp);
		if (cmd->type == BBFD_CMD_UBUS_ASYNC)
			free(cmd->ubus.args);
	}

	USP_FREE(cmd);
}

static void bbfd_cmd_enqueue(struct bbfd_cmd *cmd)
{
	pthread_mutex_lock(&cmd_queue_mutex);
	list_add_tail(&cmd->list, &cmd_queue);
	pthread_mutex_unlock(&cmd_queue_mutex);

	bbfd_thread_cmd(BBFD_THD_CMD_WAKEUP);
}

static struct bbfd_cmd * bbfd_cmd_dequeue()
{
	struct bbfd_cmd *cmd = NULL;

	pthread_mutex_lock(&cmd_queue_mutex);
	if (!list_empty(&cmd_queue)) {
		cmd = list_first_entry(&cmd_queue, struct bbfd_cmd, list);
		list_del(&cmd->list);
	}
	pthread_mutex_unlock(&cmd_queue_mutex);

	return cmd;
}

static void bbfd_thread_process_cmds()
{
	struct bbfd_cmd *c;

	while ((c = bbfd_cmd_dequeue()))
		bbfd_thread_process_cmd(c);
}

static void cmd_pipe_cb(__unused struct uloop_fd *fd, __unused unsigned events)
{
	int cmd;
	int rc;

	rc = read(cmd_pipe[0], &cmd, sizeof(cmd));
	if (rc == -1) {
		USP_LOG_Error("Read from cmd pipe failed: %s", strerror(errno));
		return;
	}

	switch (cmd) {
	case BBFD_THD_CMD_EXIT:
		uloop_end();
		break;
	case BBFD_THD_CMD_WAKEUP:
		bbfd_thread_process_cmds();
		break;
	default:
		break;
	};
}

static int bbfd_thd_call(const char *method, struct blob_buf *data, ubus_data_handler_t callback, void *cb_arg)
{
	struct bbfd_cmd *cmd = bbfd_cmd_new();
	int err;

	cmd->type = BBFD_CMD_UBUS_SYNC;
	cmd->ubus.cmd = method;
	cmd->ubus.args = data->head;
	bbfd_cmd_enqueue(cmd);

	/* wait for ubus thread to complete reqeust */
	wait_pipe_wait();

	err = cmd->err;
	if (err == USP_ERR_OK) {
		cmd->ubus.req.priv = cb_arg;
		callback(&cmd->ubus.req, 0, cmd->ubus.resp);
	}

	bbfd_cmd_free(cmd);
	return err;
}

static void bbfd_lookup_ubus_id()
{
	int rc;

	if (bbfd_ubus_id)
		return;

	rc = ubus_lookup_id(ubus_ctx, BBFD_UBUS, &bbfd_ubus_id);
	if (rc)
		USP_LOG_Error("%s: Failed to lookup " BBFD_UBUS ": %s",
			__func__, ubus_strerror(rc));
}

static void bbfd_ubus_error(struct bbfd_cmd *cmd, int ret)
{
	USP_LOG_Error("%s: cmd failed for '%s': %s",
			__func__, cmd->ubus.cmd, ubus_strerror(ret));
	bbfd_ubus_id = 0;
	cmd->err = USP_ERR_INTERNAL_ERROR;
}

static int bbfd_call(const char *method, struct blob_buf *data, ubus_data_handler_t callback, void *cb_arg)
{
	int tries = 0;
	int rc;

retry:
	bbfd_lookup_ubus_id();

	rc = ubus_invoke(ubus_ctx, bbfd_ubus_id, method, data->head, callback, cb_arg, BBFD_TIMEOUT_SYNC);
	if (rc) {
		if (!tries) {
			bbfd_ubus_id = 0;
			tries++;
			goto retry;
		}
		USP_LOG_Error("%s: Failed to invoke " BBFD_UBUS " %s: %s",
				__func__, method, ubus_strerror(rc));
		return USP_ERR_INTERNAL_ERROR;
	}
	return USP_ERR_OK;
}

static int bbfd_call_async(const char *method, struct blob_attr *data, struct ubus_request *req)
{
	int tries = 0;
	int rc;

retry:
	bbfd_lookup_ubus_id();

	rc = ubus_invoke_async(ubus_ctx, bbfd_ubus_id, method, data, req);
	if (rc) {
		if (!tries) {
			bbfd_ubus_id = 0;
			tries++;
			goto retry;
		}
		USP_LOG_Error("%s: Failed to invoke async" BBFD_UBUS " %s: %s",
				__func__, method, ubus_strerror(rc));
		return USP_ERR_INTERNAL_ERROR;
	}
	return USP_ERR_OK;
}

static int bbf_operate_err_to_usp_err(int bbf_err)
{
	int usp_err;

	switch (bbf_err) {
	case 0:
		usp_err = USP_ERR_INVALID_ARGUMENTS;
		break;
	case 1:
		usp_err = USP_ERR_OK;
		break;
	case 2:
		usp_err = USP_ERR_COMMAND_FAILURE;
		break;
	default:
		usp_err = USP_ERR_INVALID_PATH;
		break;
	}
	return usp_err;
}

static void __bbfd_operate_cb(struct bbfd_operate_result *res, struct blob_attr *msg)
{
	kv_vector_t *kv_out = res->out;
	struct blob_attr *params;
	struct blob_attr *cur;
	size_t rem;
	enum {
		P_PARAM,
		P_VALUE,
		P_FAULT,
		__P_MAX
	};
	static const struct blobmsg_policy p[__P_MAX] = {
		{ "parameter", BLOBMSG_TYPE_STRING },
		{ "value", BLOBMSG_TYPE_STRING },
		{ "fault", BLOBMSG_TYPE_INT32 }
	};

	if (!msg)
		return;

	params = get_parameters(msg);
	if (params == NULL)
		return;

	blobmsg_for_each_attr(cur, params, rem) {
		struct blob_attr *tb[__P_MAX];

		blobmsg_parse(p, __P_MAX, tb, blobmsg_data(cur), blobmsg_len(cur));
		if (tb[P_FAULT]) {
			const int fault = blobmsg_get_u32(tb[P_FAULT]);

			res->fault = bbf_operate_err_to_usp_err(fault);
			USP_LOG_Error("Fault in operate (%d)", res->fault);
			break;
		}
		if (tb[P_PARAM] && tb[P_VALUE])
			USP_ARG_Add(kv_out,
			            blobmsg_get_string(tb[P_PARAM]),
			            blobmsg_get_string(tb[P_VALUE]));
	}
}

static void bbfd_operate_cb(struct ubus_request *req, __unused int type, struct blob_attr *msg)
{
	__bbfd_operate_cb(req->priv, msg);
}

static void bbfd_async_operate_complete_cb(struct bbfd_cmd *cmd, struct blob_attr *data)
{
	struct bbfd_operate_result res = {
		.fault = USP_ERR_OK,
		.out = NULL
	};
	char *err_msg = NULL;
	int rc;

	if (data == NULL) {
		res.fault = USP_ERR_INTERNAL_ERROR;
		err_msg = "No Data";
	} else {
		res.out = USP_ARG_Create();
		__bbfd_operate_cb(&res, data);
		if (res.fault)
			err_msg = "Unknown";
	}

	rc = USP_SIGNAL_OperationComplete(cmd->inst, res.fault, err_msg, res.out);
	if (rc != USP_ERR_OK && res.out) {
		/* core won't release this memory in case of an error */
		USP_ARG_Destroy(res.out);
		USP_FREE(res.out);
	}
}

static void bbfd_async_req_complete_cb(struct ubus_request *req, int ret)
{
	struct bbfd_cmd *cmd = container_of(req, struct bbfd_cmd, ubus.req);

	if (ret == 0)
		cmd->complete_cb(cmd, cmd->ubus.resp);
	else
		bbfd_ubus_error(cmd, ret);

	bbfd_cmd_free(cmd);
}

static void bbfd_sync_req_complete_cb(struct ubus_request *req, int ret)
{
	struct bbfd_cmd *cmd = container_of(req, struct bbfd_cmd, ubus.req);

	if (ret)
		bbfd_ubus_error(cmd, ret);

	wait_pipe_wakeup();
}

static void bbfd_async_req_data_cb(struct ubus_request *req, __unused int type, struct blob_attr *msg)
{
	struct bbfd_cmd *cmd = container_of(req, struct bbfd_cmd, ubus.req);

	cmd->ubus.resp = blob_memdup(msg);
}

static void bbfd_thd_exec_sync_cmd(struct bbfd_cmd *cmd)
{
	cmd->err = bbfd_call_async(cmd->ubus.cmd, cmd->ubus.args, &cmd->ubus.req);
	if (cmd->err == USP_ERR_OK) {
		cmd->ubus.req.data_cb = bbfd_async_req_data_cb;
		cmd->ubus.req.complete_cb = bbfd_sync_req_complete_cb;
		ubus_complete_request_async(ubus_ctx, &cmd->ubus.req);
	} else {
		wait_pipe_wakeup();
	}
}

static void bbfd_thd_exec_async_cmd(struct bbfd_cmd *cmd)
{
	cmd->err = bbfd_call_async(cmd->ubus.cmd, cmd->ubus.args, &cmd->ubus.req);
	if (cmd->err == USP_ERR_OK) {

		if (cmd->inst != INVALID)
			USP_SIGNAL_OperationStatus(cmd->inst, "Active");

		cmd->ubus.req.data_cb = bbfd_async_req_data_cb;
		cmd->ubus.req.complete_cb = bbfd_async_req_complete_cb;
		ubus_complete_request_async(ubus_ctx, &cmd->ubus.req);
	} else {
		if (cmd->inst != INVALID)
			USP_SIGNAL_OperationComplete(cmd->inst, cmd->err, NULL, NULL);

		bbfd_cmd_free(cmd);
	}
}

static void schedule_timer_expired(struct uloop_timeout *t)
{
	struct bbfd_cmd_vendor *vendor = container_of(t, struct bbfd_cmd_vendor, timo);
	struct bbfd_cmd *cmd = container_of(vendor, struct bbfd_cmd, vendor);
	char *p;

	/* path is already validated by core */
	p = strrchr(vendor->path, '.');
	strcpy(&p[1], "Timer!");

	USP_SIGNAL_DataModelEvent(vendor->path, USP_ARG_Create());
	USP_SIGNAL_OperationComplete(cmd->inst, USP_ERR_OK, NULL, NULL);
	bbfd_cmd_free(cmd);
}

static void bbfd_thd_schedule_timer(struct bbfd_cmd *cmd)
{
	struct schedule_timer_args *args = &cmd->vendor.sched_timer;
	struct uloop_timeout *timo = &cmd->vendor.timo;

	timo->cb = schedule_timer_expired;
	uloop_timeout_set(timo, args->delay_sec * 1000);

	USP_SIGNAL_OperationStatus(cmd->inst, "Active");
}

static void add_data_cb(struct ubus_request *req, __unused int type, struct blob_attr *msg)
{
	struct bbfd_add_result *vadd = req->priv;
	enum {
		P_INSTANCE,
		P_FAULT,
		__P_MAX
	};
	static const struct blobmsg_policy p[__P_MAX] = {
		{ "instance", BLOBMSG_TYPE_STRING },
		{ "fault", BLOBMSG_TYPE_INT32 },
	};
	struct blob_attr *tb[__P_MAX];

	if (!msg)
		return;

	blobmsg_parse(p, __P_MAX, tb, blobmsg_data(msg), blobmsg_len(msg));

	if (tb[P_FAULT])
		vadd->fault = blobmsg_get_u32(tb[P_FAULT]);

	if (tb[P_INSTANCE])
		vadd->instance = atoi(blobmsg_get_string(tb[P_INSTANCE]));
}

static void bbfd_set_cb(struct ubus_request *req, __unused int type, struct blob_attr *msg)
{
	struct blob_attr *params;
	struct blob_attr *cur;
	size_t rem;
	int *ret = req->priv;
	enum {
		P_FAULT,
		__P_MAX
	};
	static const struct blobmsg_policy p[__P_MAX] = {
		{ "fault", BLOBMSG_TYPE_INT32 }
	};

	if (!msg)
		return;

	params = get_parameters(msg);
	if (params == NULL)
		return;

	blobmsg_for_each_attr(cur, params, rem) {
		struct blob_attr *tb[__P_MAX];

		blobmsg_parse(p, __P_MAX, tb, blobmsg_data(cur), blobmsg_len(cur));
		if (tb[P_FAULT]) {
			const int fault = blobmsg_get_u32(tb[P_FAULT]);

			USP_LOG_Error("fault occoured |%d|", fault);
			*ret = fault;
		}
	}
}

static void del_data_cb(struct ubus_request *req, int type, struct blob_attr *msg)
{
	bbfd_set_cb(req, type, msg);
}

static int bbfd_operate_async(dm_req_t *req, kv_vector_t *input_args, int instance)
{
	struct bbfd_cmd *cmd = bbfd_cmd_new();

	cmd->type = BBFD_CMD_UBUS_ASYNC;
	cmd->inst = instance;
	cmd->ubus.cmd = "operate";
	cmd->complete_cb = bbfd_async_operate_complete_cb;

	bbfd_operate_param_init(&gb, req->path, input_args);
	cmd->ubus.args = blob_memdup(gb.head);

	bbfd_cmd_enqueue(cmd);

	return USP_ERR_OK;
}

static int bbfd_operate_sync(dm_req_t *req, __unused char *command_key, kv_vector_t *input_args, kv_vector_t *output_args)
{
	struct bbfd_operate_result res = {
		.fault = USP_ERR_OK,
		.out = output_args,
	};
	int fault;

	bbfd_operate_param_init(&gb, req->path, input_args);

	fault = bbfd_thd_call("operate", &gb, bbfd_operate_cb, &res);
	if (fault == USP_ERR_OK)
		fault = res.fault;

	return fault;
}

static void get_instances_cb(struct ubus_request *req, __unused int type, struct blob_attr *msg)
{
	str_vector_t *vec = req->priv;
	struct blob_attr *params;
	struct blob_attr *cur;
	size_t rem;
	enum {
		P_PARAM,
		__P_MAX
	};
	static const struct blobmsg_policy p[__P_MAX] = {
		{ "parameter", BLOBMSG_TYPE_STRING }
	};

	if (!msg)
		return;

	params = get_parameters(msg);
	if (params == NULL)
		return;

	blobmsg_for_each_attr(cur, params, rem) {
		struct blob_attr *tb[__P_MAX];

		blobmsg_parse(p, __P_MAX, tb, blobmsg_data(cur), blobmsg_len(cur));
		if (tb[P_PARAM])
			STR_VECTOR_Add(vec, blobmsg_get_string(tb[P_PARAM]));
	}
}

static void get_values_cb(struct ubus_request *req, __unused int type, struct blob_attr *msg)
{
	struct bbfd_get_result *vget = req->priv;
	struct blob_attr *params;
	struct blob_attr *cur;
	size_t rem;
	enum {
		P_PARAM,
		P_VALUE,
		__P_MAX
	};
	static const struct blobmsg_policy p[__P_MAX] = {
		{ "parameter", BLOBMSG_TYPE_STRING },
		{ "value", BLOBMSG_TYPE_STRING }
	};

	if (!msg)
		return;

	params = get_parameters(msg);
	if (params == NULL)
		return;

	blobmsg_for_each_attr(cur, params, rem) {
		struct blob_attr *tb[__P_MAX];

		blobmsg_parse(p, __P_MAX, tb, blobmsg_data(cur), blobmsg_len(cur));
		if (!tb[P_PARAM] || !tb[P_VALUE])
			continue;

		strncpyt(vget->buf, blobmsg_get_string(tb[P_VALUE]), vget->buflen);

		/* get queries from core come only for single parameters */
		break;
	}
}

static void schema_get_cb(struct ubus_request *req, __unused int type, struct blob_attr *msg)
{
	kv_vector_t *kv = req->priv;
	struct blob_attr *params;
	struct blob_attr *cur;
	size_t rem;
	enum {
		P_PARAM,
		P_WRITEABLE,
		P_TYPE,
		__P_MAX
	};
	static const struct blobmsg_policy p[__P_MAX] = {
		{ "parameter", BLOBMSG_TYPE_STRING },
		{ "writeable", BLOBMSG_TYPE_STRING },
		{ "type", BLOBMSG_TYPE_STRING },
	};

	if (!msg)
		return;

	params = get_parameters(msg);
	if (params == NULL)
		return;

	blobmsg_for_each_attr(cur, params, rem) {
		struct blob_attr *tb[__P_MAX];
		char val[MAX_DM_PATH];
		char path[MAX_DM_PATH];
		size_t plen;

		blobmsg_parse(p, __P_MAX, tb, blobmsg_data(cur), blobmsg_len(cur));
		if (!tb[P_PARAM] || !tb[P_TYPE] || !tb[P_WRITEABLE])
			continue;

		strncpyt(path, blobmsg_get_string(tb[P_PARAM]), sizeof(path));
		plen = strlen(path);

		if (path[plen - 1] == '.' && path[plen - 2] != '}')
			continue;

		if (path[plen - 1] == '.')
			path[plen - 1] = 0;

		USP_SNPRINTF(val, MAX_DM_PATH, "%s %s",
		             blobmsg_get_string(tb[P_TYPE]),
		             blobmsg_get_string(tb[P_WRITEABLE]));
		USP_ARG_Add(kv, path, val);
	}
}

static int bbfd_get_object_paths(kv_vector_t *kv)
{
	int fault;

	bbfd_get_param_init(&gb, DEVICE_ROOT, true);
	fault = bbfd_call("get_schema", &gb, schema_get_cb, kv);

	return fault;
}

static int bbfd_set_value(dm_req_t *req, char *value)
{
	int fault = USP_ERR_OK;
	int ret;

	bbfd_param_init(&gb, req->path);
	blobmsg_add_string(&gb, "value", value);

	ret = bbfd_thd_call("set", &gb, bbfd_set_cb, &fault);
	if (ret == USP_ERR_OK)
		ret = fault;

	return ret;
}

static int __bbfd_get_value(const char *path, char *buf, int len, int threaded)
{
	struct bbfd_get_result res = {
		.fault = USP_ERR_OK,
		.buf = buf,
		.buflen = len
	};
	int fault;

	res.buf[0] = 0;
	bbfd_get_param_init(&gb, path, true);

	if (threaded)
		fault = bbfd_thd_call("get_values", &gb, get_values_cb, &res);
	else
		fault = bbfd_call("get_values", &gb, get_values_cb, &res);

	if (fault == USP_ERR_OK)
		fault = res.fault;

	return fault;
}

static int bbfd_get_value(dm_req_t *req, char *buf, int len)
{
	return __bbfd_get_value(req->path, buf, len, 1);
}

static int bbfd_add_object(dm_req_t *req)
{
	struct bbfd_add_result vadd = {USP_ERR_OK, 0};
	int fault;

	bbfd_param_init(&gb, req->path);
	fault = bbfd_thd_call("add_object", &gb, add_data_cb, &vadd);
	if (fault == USP_ERR_OK)
		fault = vadd.fault;

	return fault;
}

static int bbfd_del_object(dm_req_t *req)
{
	int fault;
	int err;

	bbfd_param_init(&gb, req->path);
	fault = bbfd_thd_call("del_object", &gb, del_data_cb, &err);
	if (fault == USP_ERR_OK)
		fault = err;

	return fault;
}

static void bbfd_operation_to_usp_format(char *dst, size_t len, const char *src)
{
	size_t i, n;

	for (n = i = 0; src[i] && n < len - 3; i++) {
		if (src[i] == '*') {
			dst[n++] = '{';
			dst[n++] = 'i';
			dst[n++] = '}';
		} else {
			dst[n++] = src[i];
		}
	}
	dst[n++] = '(';
	dst[n++] = ')';
	dst[n] = 0;
}

static int parse_arg_array(struct blob_attr *array, char **dst)
{
	struct blob_attr *cur;
	int n = 0;
	size_t rem;

	blobmsg_for_each_attr(cur, array, rem)
		dst[n++] = blobmsg_get_string(cur);

	return n;
}

static void bbfd_register_operation_params(const char *op, struct blob_attr *args)
{
	char *args_in[BBFD_OPERATE_ARGS_MAX];
	char *args_out[BBFD_OPERATE_ARGS_MAX];
	struct blob_attr *cur;
	int n_in = 0;
	int n_out = 0;
	size_t rem;

	blobmsg_for_each_attr(cur, args, rem) {
		const char *name = blobmsg_name(cur);

		if (!strcmp(name, "in"))
			n_in = parse_arg_array(cur, args_in);
		else if (!strcmp(name, "out"))
			n_out = parse_arg_array(cur, args_out);
	}

	USP_REGISTER_OperationArguments((char *)op, args_in, n_in, args_out, n_out);
}

static void get_operates_cb(struct ubus_request *req, __unused int type, struct blob_attr *msg)
{
	struct blob_attr *params;
	struct blob_attr *cur;
	size_t rem;
	enum {
		P_PARAM,
		P_ARGS,
		P_TYPE,
		__P_MAX
	};
	static const struct blobmsg_policy p[__P_MAX] = {
		{ "parameter", BLOBMSG_TYPE_STRING },
		{ "args", BLOBMSG_TYPE_TABLE },
		{ "type", BLOBMSG_TYPE_STRING },
	};

	if (!msg)
		return;

	params = get_parameters(msg);
	if (params == NULL)
		return;

	blobmsg_for_each_attr(cur, params, rem) {
		struct blob_attr *tb[__P_MAX];

		blobmsg_parse(p, __P_MAX, tb, blobmsg_data(cur), blobmsg_len(cur));
		if (tb[P_PARAM] && tb[P_TYPE]) {
			const char *path = blobmsg_get_string(tb[P_PARAM]);
			const char *type = blobmsg_get_string(tb[P_TYPE]);
			char op[MAX_DM_PATH];

			bbfd_operation_to_usp_format(op, sizeof(op) - 1, path);

			if (!strcmp(type, "sync"))
				USP_REGISTER_SyncOperation(op, bbfd_operate_sync);
			else
				USP_REGISTER_AsyncOperation(op, bbfd_operate_async, NULL);

			bbfd_register_operation_params(op, tb[P_ARGS]);
		}
	}
}

static int get_dm_type(const char *type)
{
	if (strcmp(type, "xsd:string") == 0)
		return DM_STRING;
	else if(strcmp(type, "xsd:unsignedInt") == 0)
		return DM_UINT;
	else if (strcmp(type, "xsd:int") == 0)
		return DM_INT;
	else if (strcmp(type, "xsd:unsignedLong") == 0)
		return DM_ULONG;
	else if (strcmp(type, "xsd:Long") == 0)
		return DM_ULONG;
	else if (strcmp(type, "xsd:boolean") == 0)
		return DM_BOOL;
	else if (strcmp(type, "xsd:dateTime") == 0)
		return DM_DATETIME;

	return DM_STRING;
}

static void bbfd_register_parameter(char *spath, char *writeable, char *dmtype)
{
	const int type = get_dm_type(dmtype);

	if (writeable[0] == '1')
		USP_REGISTER_VendorParam_ReadWrite(spath, bbfd_get_value, bbfd_set_value, NULL, type);
	else
		USP_REGISTER_VendorParam_ReadOnly(spath, bbfd_get_value, type);
}


static void bbfd_register_uniq_param(char *spath, kv_vector_t *kv)
{
	int i;
	int key_count = 0;
	char **unique_keys = NULL, *temp_val, *t;
	char *tok, *save;

	t = USP_ARG_Get(kv, spath, NULL);
	if (t == NULL)
		return;

	temp_val = USP_STRDUP(t);

	tok = strtok_r(temp_val, ";", &save);
	while (tok != NULL) {
		if (key_count == 0) {
			unique_keys = USP_MALLOC(sizeof(char *));
		} else {
			unique_keys = USP_REALLOC(unique_keys, sizeof(char *) * (key_count + 1));
		}
		if (unique_keys == NULL) {
			USP_LOG_Error("Out of memory!");
			return;
		}
		unique_keys[key_count] = USP_STRDUP(tok);
		key_count++;
		tok = strtok_r(NULL, ";", &save);
	}
	USP_SAFE_FREE(temp_val);

	if (key_count)
		USP_REGISTER_Object_UniqueKey(spath, unique_keys, key_count);

	for (i = 0; i < key_count; i++) {
		USP_SAFE_FREE(unique_keys[i]);
	}
	USP_SAFE_FREE(unique_keys);
}

/* NOTE: ugly hack to register uniq keys
 */
static void register_bbfd_schema_uniq_keys(kv_vector_t *kv)
{
	kv_vector_t uniq_kv;
	int i;

	USP_ARG_Init(&uniq_kv);
	USP_ARG_Add(&uniq_kv, "Device.DeviceInfo.VendorConfigFile.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.DeviceInfo.ProcessStatus.Process.{i}", "PID");
	USP_ARG_Add(&uniq_kv, "Device.DeviceInfo.TemperatureStatus.TemperatureSensor.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.DeviceInfo.VendorLogFile.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.DeviceInfo.Location.{i}", "Source;ExternalSource");
	USP_ARG_Add(&uniq_kv, "Device.LEDs.LED.{i}.CycleElement.{i}", "Order");
	USP_ARG_Add(&uniq_kv, "Device.BASAPM.MeasurementEndpoint.{i}", "MeasurementAgent");
	USP_ARG_Add(&uniq_kv, "Device.DeviceInfo.DeviceImageFile.{i}", "Location");
	USP_ARG_Add(&uniq_kv, "Device.DeviceInfo.FirmwareImage.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.DSL.Line.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.DSL.Channel.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.DSL.BondingGroup.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.DSL.BondingGroup.{i}.BondedChannel.{i}", "Channel");
	USP_ARG_Add(&uniq_kv, "Device.FAST.Line.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.Optical.Interface.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.Cellular.Interface.{i}","Name");
	USP_ARG_Add(&uniq_kv, "Device.Cellular.AccessPoint.{i}", "Interface");
	USP_ARG_Add(&uniq_kv, "Device.ATM.Link.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.PTM.Link.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.Ethernet.Interface.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.Ethernet.Link.{i}", "Name;MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.Ethernet.VLANTermination.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.Ethernet.RMONStats.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.Ethernet.LAG.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.USB.Interface.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.USB.Port.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.USB.USBHosts.Host.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.USB.USBHosts.Host.{i}.Device.{i}", "DeviceNumber");
	USP_ARG_Add(&uniq_kv, "Device.USB.USBHosts.Host.{i}.Device.{i}.Configuration.{i}", "ConfigurationNumber");
	USP_ARG_Add(&uniq_kv, "Device.USB.USBHosts.Host.{i}.Device.{i}.Configuration.{i}.Interface.{i}", "InterfaceNumber");
	USP_ARG_Add(&uniq_kv, "Device.HPNA.Interface.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.HPNA.Interface.{i}.AssociatedDevice.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.MoCA.Interface.{i}.QoS.FlowStats.{i}", "FlowID");
	USP_ARG_Add(&uniq_kv, "Device.MoCA.Interface.{i}.AssociatedDevice.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.Ghn.Interface.{i}.AssociatedDevice.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.Ghn.Interface.{i}.SMMaskedBand.{i}", "BandNumber");
	USP_ARG_Add(&uniq_kv, "Device.HomePlug.Interface.{i}.AssociatedDevice.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.UPA.Interface.{i}.AssociatedDevice.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.UPA.Interface.{i}.BridgeFor.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.Radio.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.SSID.{i}", "Name;BSSID");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.AccessPoint.{i}", "SSIDReference");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.AccessPoint"".{i}.AssociatedDevice.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.AccessPoint"".{i}.AC.{i}", "AccessCategory");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.EndPoint.{i}", "SSIDReference");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.EndPoint.{i}.Profile.{i}", "SSID");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.EndPoint"".{i}.AC.{i}", "AccessCategory");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.MultiAP.APDevice.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.MultiAP.APDevice.{i}.Radio.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.MultiAP.APDevice.{i}.Radio.{i}.AP.{i}", "BSSID");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.MultiAP.APDevice.{i}.Radio.{i}.AP.{i}.AssociatedDevice.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.MultiAP.APDevice.{i}.Radio.{i}.AP.{i}.AssociatedDevice.{i}.SteeringHistory.{i}", "Time;APOrigin;APDestination");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}", "ID");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}.Radio.{i}", "ID");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}.Radio.{i}.Capabilities.CapableOperatingClassProfile.{i}", "Class");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}.Radio.{i}.CurrentOperatingClassProfile.{i}", "Class");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}.Radio.{i}.BSS.{i}", "BSSID");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}.Radio.{i}.BSS.{i}.STA.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}.Radio.{i}.ScanResult.{i}.OpClassScan.{i}", "OperatingClass");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}.Radio.{i}.ScanResult.{i}.OpClassScan.{i}.ChannelScan.{i}", "Channel");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}.Radio.{i}.ScanResult.{i}.OpClassScan.{i}.ChannelScan.{i}.NeighborBSS.{i}", "BSSID");
	USP_ARG_Add(&uniq_kv, "Device.WiFi.DataElements.Network.Device.{i}.Radio.{i}.UnassociatedSTA.{i}", "MACAddress");
	USP_ARG_Add(&uniq_kv, "Device.ZigBee.Interface.{i}.AssociatedDevice.{i}", "IEEEAddress;NetworkAddress");
	USP_ARG_Add(&uniq_kv, "Device.ZigBee.ZDO.{i}", "IEEEAddress;NetworkAddress");
	USP_ARG_Add(&uniq_kv, "Device.ZigBee.ZDO.{i}.Network.Neighbor.{i}", "Neighbor");
	USP_ARG_Add(&uniq_kv, "Device.ZigBee.ZDO.{i}.NodeManager.RoutingTable.{i}", "DestinationAddress");
	USP_ARG_Add(&uniq_kv, "Device.Bridging.Bridge.{i}.Port.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.Bridging.Bridge.{i}.VLAN.{i}", "VLANID");
	USP_ARG_Add(&uniq_kv, "Device.Bridging.Bridge.{i}.VLANPort.{i}", "VLAN;Port");
	USP_ARG_Add(&uniq_kv, "Device.PPP.Interface.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.IP.Interface.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.IP.Interface.{i}.IPv4Address.{i}", "IPAddress;SubnetMask");
	USP_ARG_Add(&uniq_kv, "Device.IP.Interface.{i}.IPv6Address.{i}", "IPAddress;Prefix");
	USP_ARG_Add(&uniq_kv, "Device.IP.Interface.{i}.IPv6Prefix.{i}", "Prefix");
	USP_ARG_Add(&uniq_kv, "Device.IP.ActivePort.{i}", "LocalIPAddress;LocalPort;RemoteIPAddress;RemotePort");
	USP_ARG_Add(&uniq_kv, "Device.LLDP.Discovery.Device.{i}", "ChassisIDSubtype;ChassisID");
	USP_ARG_Add(&uniq_kv, "Device.LLDP.Discovery.Device.{i}.Port.{i}", "PortIDSubtype;PortID");
	USP_ARG_Add(&uniq_kv, "Device.LLDP.Discovery.Device.{i}.DeviceInformation.VendorSpecific.{i}", "OrganizationCode;InformationType");
	USP_ARG_Add(&uniq_kv, "Device.IPsec.Profile.{i}.SentCPAttr.{i}", "Type");
	USP_ARG_Add(&uniq_kv, "Device.IPsec.Tunnel.{i}", "TunnelInterface;TunneledInterface");
	USP_ARG_Add(&uniq_kv, "Device.IPsec.IKEv2SA.{i}", "Tunnel");
	USP_ARG_Add(&uniq_kv, "Device.GRE.Tunnel.{i}.Interface.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.VXLAN.Tunnel.{i}.Interface.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}", "Identifier");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}.TaskCapability.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}.TaskCapability.{i}.Registry.{i}", "RegistryEntry");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}.Schedule.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}.Schedule.{i}.Action.{i}.Option.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}.Task.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}.Task.{i}.Registry.{i}", "RegistryEntry");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}.Task.{i}.Option.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}.CommunicationChannel.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.MeasurementAgent.{i}.Instruction.{i}.MeasurementSuppression.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.Report.{i}.Result.{i}", "ScheduleName;ActionName;StartTime");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.Report.{i}.Result.{i}.Option.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.Report.{i}.Result.{i}.Conflict.{i}", "ScheduleName;ActionName;TaskName");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.Report.{i}.Result.{i}.ReportTable.{i}.Registry.{i}", "RegistryEntry");
	USP_ARG_Add(&uniq_kv, "Device.LMAP.Event.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.SoftwareModules.ExecEnv.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.SoftwareModules.DeploymentUnit.{i}", "UUID;Version;ExecutionEnvRef");
	USP_ARG_Add(&uniq_kv, "Device.SoftwareModules.ExecutionUnit.{i}", "EUID");
	USP_ARG_Add(&uniq_kv, "Device.IoTCapability.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.Routing.Router.{i}.IPv4Forwarding.{i}", "DestIPAddress;DestSubnetMask;ForwardingPolicy;GatewayIPAddress;Interface;ForwardingMetric");
	USP_ARG_Add(&uniq_kv, "Device.Routing.Router.{i}.IPv6Forwarding.{i}", "DestIPPrefix;ForwardingPolicy;NextHop;Interface;ForwardingMetric");
	USP_ARG_Add(&uniq_kv, "Device.Routing.RouteInformation.InterfaceSetting.{i}", "Interface");
	USP_ARG_Add(&uniq_kv, "Device.NeighborDiscovery.InterfaceSetting.{i}", "Interface");
	USP_ARG_Add(&uniq_kv, "Device.RouterAdvertisement.InterfaceSetting.{i}", "Interface");
	USP_ARG_Add(&uniq_kv, "Device.RouterAdvertisement.InterfaceSetting.{i}.Option.{i}", "Tag");
	USP_ARG_Add(&uniq_kv, "Device.QoS.Shaper.{i}", "Interface");
	USP_ARG_Add(&uniq_kv, "Device.Hosts.Host.{i}", "PhysAddress");
	USP_ARG_Add(&uniq_kv, "Device.Hosts.Host.{i}.IPv4Address.{i}", "IPAddress");
	USP_ARG_Add(&uniq_kv, "Device.Hosts.Host.{i}.IPv6Address.{i}", "IPAddress");
	USP_ARG_Add(&uniq_kv, "Device.DNS.Client.Server.{i}", "DNSServer");
	USP_ARG_Add(&uniq_kv, "Device.DNS.Relay.Forwarding.{i}", "DNSServer");
	USP_ARG_Add(&uniq_kv, "Device.DNS.SD.Service.{i}", "InstanceName");
	USP_ARG_Add(&uniq_kv, "Device.DNS.SD.Service.{i}.TextRecord.{i}", "Key");
	USP_ARG_Add(&uniq_kv, "Device.NAT.InterfaceSetting.{i}", "Interface");
	USP_ARG_Add(&uniq_kv, "Device.NAT.PortMapping.{i}", "RemoteHost;ExternalPort;Protocol");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv4.Client.{i}", "Interface");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv4.Client.{i}.SentOption.{i}", "Tag");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv4.Client.{i}.ReqOption.{i}", "Tag");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv4.Server.Pool.{i}.StaticAddress.{i}", "Chaddr");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv4.Server.Pool.{i}.Option.{i}", "Tag");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv4.Server.Pool.{i}.Client.{i}", "Chaddr");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv4.Server.Pool.{i}.Client.{i}.IPv4Address.{i}", "IPAddress");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv4.Server.Pool.{i}.Client.{i}.Option.{i}", "Tag");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Client.{i}", "Interface");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Client.{i}.Server.{i}", "SourceAddress");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Client.{i}.SentOption.{i}", "Tag");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Client.{i}.ReceivedOption.{i}", "Tag");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Server.Pool.{i}", "Order");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Server.Pool.{i}.Client.{i}", "SourceAddress");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Server.Pool.{i}.Client.{i}.IPv6Address.{i}", "IPAddress");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Server.Pool.{i}.Client.{i}.IPv6Prefix.{i}", "Prefix");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Server.Pool.{i}.Client.{i}.Option.{i}", "Tag");
	USP_ARG_Add(&uniq_kv, "Device.DHCPv6.Server.Pool.{i}.Option.{i}", "Tag");
	USP_ARG_Add(&uniq_kv, "Device.Users.User.{i}", "Username");
	USP_ARG_Add(&uniq_kv, "Device.SmartCardReaders.SmartCardReader.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.UPnP.Discovery.RootDevice.{i}", "UUID");
	USP_ARG_Add(&uniq_kv, "Device.UPnP.Discovery.Device.{i}", "UUID");
	USP_ARG_Add(&uniq_kv, "Device.UPnP.Discovery.Service.{i}", "USN");
	USP_ARG_Add(&uniq_kv, "Device.UPnP.Description.DeviceDescription.{i}", "URLBase");
	USP_ARG_Add(&uniq_kv, "Device.UPnP.Description.DeviceInstance.{i}", "UDN");
	USP_ARG_Add(&uniq_kv, "Device.UPnP.Description.ServiceInstance.{i}", "ParentDevice;ServiceId");
	USP_ARG_Add(&uniq_kv, "Device.Firewall.Level.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.Firewall.Chain.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.PeriodicStatistics.SampleSet.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.PeriodicStatistics.SampleSet.{i}.Parameter.{i}", "Reference");
	USP_ARG_Add(&uniq_kv, "Device.FaultMgmt.SupportedAlarm.{i}", "EventType;ProbableCause;SpecificProblem;PerceivedSeverity");
	USP_ARG_Add(&uniq_kv, "Device.FaultMgmt.CurrentAlarm.{i}", "AlarmIdentifier");
	USP_ARG_Add(&uniq_kv, "Device.FaultMgmt.HistoryEvent.{i}", "EventTime;AlarmIdentifier");
	USP_ARG_Add(&uniq_kv, "Device.FaultMgmt.ExpeditedEvent.{i}", "AlarmIdentifier");
	USP_ARG_Add(&uniq_kv, "Device.FaultMgmt.QueuedEvent.{i}", "AlarmIdentifier");
	USP_ARG_Add(&uniq_kv, "Device.FAP.PerfMgmt.Config.{i}", "URL;PeriodicUploadInterval;PeriodicUploadTime");
	USP_ARG_Add(&uniq_kv, "Device.XMPP.Connection.{i}", "Username;Domain;Resource");
	USP_ARG_Add(&uniq_kv, "Device.XMPP.Connection.{i}.Server.{i}", "ServerAddress;Port");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.Interface.{i}", "InterfaceId");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.Interface.{i}.Link.{i}", "InterfaceId;IEEE1905Id");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.NetworkTopology.IEEE1905Device.{i}", "IEEE1905Id");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.NetworkTopology.IEEE1905Device.{i}.IPv4Address.{i}", "MACAddress;IPv4Address");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.NetworkTopology.IEEE1905Device.{i}.IPv6Address.{i}", "MACAddress;IPv6Address");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.NetworkTopology.IEEE1905Device.{i}.Interface.{i}", "InterfaceId");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.NetworkTopology.IEEE1905Device.{i}.NonIEEE1905Neighbor.{i}", "LocalInterface;NeighborInterfaceId");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.NetworkTopology.IEEE1905Device.{i}.L2Neighbor.{i}", "LocalInterface;NeighborInterfaceId");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.NetworkTopology.IEEE1905Device.{i}.IEEE1905Neighbor.{i}", "LocalInterface;NeighborDeviceId");
	USP_ARG_Add(&uniq_kv, "Device.IEEE1905.AL.NetworkTopology.IEEE1905Device.{i}.IEEE1905Neighbor.{i}.Metric.{i}", "NeighborMACAddress");
	USP_ARG_Add(&uniq_kv, "Device.DynamicDNS.Client.{i}", "Server;Username");
	USP_ARG_Add(&uniq_kv, "Device.DynamicDNS.Client.{i}.Hostname.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.DynamicDNS.Server.{i}", "Name");
	USP_ARG_Add(&uniq_kv, "Device.LEDs.LED.{i}", "Name");

	for (i = 0; i < kv->num_entries; ++i) {
		char *spath = kv->vector[i].key;

		if (!strcmp(kv->vector[i].value, "xsd:object"))
			bbfd_register_uniq_param(spath, &uniq_kv);
	}
	USP_ARG_Destroy(&uniq_kv);
}

static void register_bbfd_schema(kv_vector_t *kv)
{
	int i;

	for (i = 0; i < kv->num_entries; ++i) {
		char *spath = kv->vector[i].key;
		char *type;
		char *write;

		TEXT_UTILS_KeyValueFromString(kv->vector[i].value, &type, &write);
		if (type == NULL)
			continue;

		if (!strcmp(type, "xsd:object"))
			USP_REGISTER_Object(spath, NULL, bbfd_add_object,
					NULL, NULL, bbfd_del_object, NULL);
		else
			bbfd_register_parameter(spath, write, type);
	}
}

static void bbfd_register_schema()
{
	kv_vector_t kv_vec;

	USP_ARG_Init(&kv_vec);
	bbfd_get_object_paths(&kv_vec);
	register_bbfd_schema(&kv_vec);
	register_bbfd_schema_uniq_keys(&kv_vec);
	USP_ARG_Destroy(&kv_vec);
}

void bbfd_register_operate_schema()
{
	blob_buf_init(&gb, 0);
	bbfd_call("get_operates", &gb, get_operates_cb, NULL);
}

static void bbfd_register_instances()
{
	int i;

	STR_VECTOR_Init(&g_instances);

	bbfd_get_param_init(&gb, DEVICE_ROOT, true);
	bbfd_call("get_instances", &gb, get_instances_cb, &g_instances);

	for (i = 0; i < g_instances.num_entries; i++)
		USP_DM_InformInstance(g_instances.vector[i]);
}

static void instances_update(str_vector_t *new_instances)
{
	int i, found;
	char *p;

	for (i = 0; i < g_instances.num_entries; i++) {
		p = g_instances.vector[i];

		found = STR_VECTOR_Find(new_instances, p);
		if (found == INVALID) {
			USP_LOG_Debug("Object Instance (%s) deleted", p);
			USP_SIGNAL_ObjectDeleted(p);
		}
	}

	for (i = 0; i < new_instances->num_entries; i++) {
		p = new_instances->vector[i];

		found = STR_VECTOR_Find(&g_instances, p);
		if (found == INVALID) {
			USP_LOG_Debug("Object Instance (%s) added", p);
			USP_SIGNAL_ObjectAdded(p);
		}
	}

	STR_VECTOR_Destroy(&g_instances);
	g_instances = *new_instances;
}

static void get_instances_complete_cb(struct bbfd_cmd *cmd, struct blob_attr *data)
{
	str_vector_t new_instances;

	STR_VECTOR_Init(&new_instances);

	cmd->ubus.req.priv = &new_instances;
	get_instances_cb(&cmd->ubus.req, 0, data);

	instances_update(&new_instances);
}

/* TODO: let bbfd monitor internally and use ubus events to signal changes
 */
static void bbfd_query_instances()
{
	struct bbfd_cmd *cmd = bbfd_cmd_new();
	struct blob_buf b = {};

	cmd->type = BBFD_CMD_UBUS_ASYNC;
	cmd->ubus.cmd = "get_instances";
	cmd->complete_cb = get_instances_complete_cb;

	bbfd_get_param_init(&b, DEVICE_ROOT, true);
	cmd->ubus.args = blob_memdup(b.head);
	blob_buf_free(&b);

	bbfd_cmd_enqueue(cmd);
}

static void monitor_instances_cb(struct uloop_timeout *t)
{
	bbfd_query_instances();
	uloop_timeout_set(t, VALUE_CHANGE_POLL_PERIOD_MS);
}

struct ubus_context * bbfd_ubus_init()
{
	struct ubus_context *ctx = ubus_connect(NULL);

	if (!ctx)
		USP_LOG_Error("%s: ubus_connect failed",__func__);

	return ctx;
}

static void *bbfd_thread(__unused void *arg)
{
	struct uloop_fd cmd_fd = {
		.fd = cmd_pipe[0],
		.cb = cmd_pipe_cb,
	};

	uloop_init();
	uloop_fd_add(&cmd_fd, ULOOP_READ);
	uloop_timeout_set(&monitor_instances_timer, VALUE_CHANGE_POLL_PERIOD_MS);

	ubus_add_uloop(ubus_ctx);
	uloop_run();

	uloop_done();
	close(wait_pipe[1]);
	return NULL;
}

static int __bbfd_operate_sync(const char *path)
{
	dm_req_t req = {
		.path = (char *)path
	};
	kv_vector_t input_args, output_args;
	int res;

	KV_VECTOR_Init(&input_args);
	KV_VECTOR_Init(&output_args);

	res = bbfd_operate_sync(&req, NULL, &input_args, &output_args);
	KV_VECTOR_Destroy(&input_args);
	KV_VECTOR_Destroy(&output_args);
	return res;
}

static int bbfd_factory_reset()
{
	return __bbfd_operate_sync("Device.FactoryReset()");
}

static int bbfd_reboot()
{
	return system("reboot");
}

static int bbfd_get_sync(const char *path, char *buf, int len)
{
	/* only use threaded when vendor_start has been called by
	 * the core and the thread is running
	 */
	const int threaded = cmd_pipe[0] != -1;

	return __bbfd_get_value(path, buf, len, threaded);
}

static int bbfd_get_serialno(char *buf, int len)
{
	return bbfd_get_sync("Device.DeviceInfo.SerialNumber", buf, len);
}

static int bbfd_get_sw_version(char *buf, int len)
{
	return bbfd_get_sync("Device.DeviceInfo.SoftwareVersion", buf, len);
}

static int bbfd_get_hw_version(char *buf, int len)
{
	return bbfd_get_sync("Device.DeviceInfo.HardwareVersion", buf, len);
}

static void bbfd_register_vendor_hooks()
{
	vendor_hook_cb_t callbacks = {
		.get_agent_serial_number_cb = bbfd_get_serialno,
		.reboot_cb = bbfd_reboot,
		.factory_reset_cb = bbfd_factory_reset,
		.get_active_software_version_cb = bbfd_get_sw_version,
		.get_hardware_version_cb = bbfd_get_hw_version
	};

	USP_REGISTER_CoreVendorHooks(&callbacks);
}

static void bbfd_register_events()
{
	USP_REGISTER_Event("Device.LocalAgent.TransferComplete!");
	USP_REGISTER_Event("Device.LocalAgent.Controller.{i}.Timer!");
}

static int bbfd_operate_schedule_timer(dm_req_t *req, kv_vector_t *input_args, int instance)
{
	struct bbfd_cmd *cmd = bbfd_cmd_new();
	char *delay_sec;

	cmd->type = BBFD_CMD_SCHEDULE_TIMER;
	cmd->inst = instance;

	strncpyt(cmd->vendor.path, req->path, sizeof(cmd->vendor.path));
	delay_sec = KV_VECTOR_Get(input_args, "DelaySeconds", NULL, 0);
	if (delay_sec)
		cmd->vendor.sched_timer.delay_sec = atoi(delay_sec);

	bbfd_cmd_enqueue(cmd);

	return USP_ERR_OK;
}

static void bbfd_register_vendor_operates()
{
	static const char *name = "Device.LocalAgent.Controller.{i}.ScheduleTimer()";
	static const char *in_args[] = { "DelaySeconds" };

	USP_REGISTER_AsyncOperation((char *)name, bbfd_operate_schedule_timer, NULL);
	USP_REGISTER_OperationArguments((char *)name,
					(char **)in_args, ARRAY_SIZE(in_args),
					NULL, 0);
}

int vendor_bbfd_init()
{
	if (is_running_cli_local_command)
		return USP_ERR_OK;

	ubus_ctx = bbfd_ubus_init();
	if (ubus_ctx == NULL)
		return USP_ERR_INTERNAL_ERROR;

	bbfd_register_schema();
	bbfd_register_operate_schema();
	bbfd_register_vendor_hooks();
	bbfd_register_vendor_operates();
	bbfd_register_events();
	bbfd_register_instances();

	pthread_mutex_init(&cmd_queue_mutex, NULL);

	return USP_ERR_OK;
}

int vendor_bbfd_stop()
{
	if (is_running_cli_local_command)
		return USP_ERR_OK;

	bbfd_thread_cmd(BBFD_THD_CMD_EXIT);
	wait_pipe_wait();

	close(wait_pipe[0]);
	pipe_deinit(cmd_pipe);
	ubus_free(ubus_ctx);
	STR_VECTOR_Destroy(&g_instances);
	blob_buf_free(&gb);

	return USP_ERR_OK;
}

int vendor_bbfd_start()
{
	if (is_running_cli_local_command)
		return USP_ERR_OK;

	if (pipe_init(cmd_pipe) || pipe_init(wait_pipe))
		return USP_ERR_INTERNAL_ERROR;

	return OS_UTILS_CreateThread(bbfd_thread, NULL);
}
