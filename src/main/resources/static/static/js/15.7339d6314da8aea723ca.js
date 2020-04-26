webpackJsonp([15],{czpJ:function(e,t){},"u/bs":function(e,t,s){"use strict";Object.defineProperty(t,"__esModule",{value:!0});s("0xDb");var r={data:function(){return{bootstrap:{servers:"192.168.0.105:9092"},bootstrap_servers:{home:"192.168.0.105:9092"},consumers:[{groupId:"KMOffsetCache-localhost.localdomain",isSimpleConsumerGroup:!1}]}},mounted:function(){this.consumers=[]},created:function(){this.getKafkaBootstrapServers()},watch:{},methods:{getConsumerGroups:function(){var e=this;e.$http.get(e.api.getConsumerGroups,{params:{"bootstrap.servers":e.bootstrap.servers}},function(t){0==t.code?(e.consumers=t.content.all.value,e.$message({type:"success",message:"查询成功",duration:2e3})):e.$message({type:"error",message:t.msg,duration:2e3})},function(t){e.$message({type:"warning",message:"请求异常",duration:1e3})})},deleteConsumerGroup:function(e,t){var s=this;this.$confirm("是否删除该条数据？","提示",{confirmButtonText:"确定",cancelButtonText:"取消",center:!0}).then(function(){s.$http.delete(s.api.deleteConsumerGroup,{params:{"bootstrap.servers":e,group:t}},function(e){0==e.code?1==e.content?(s.$message({type:"success",message:"删除成功",duration:2e3}),s.getConsumerGroups()):s.$message({type:"warning",message:"删除失败",duration:2e3}):s.$message({type:"error",message:e.msg,duration:2e3})},function(e){console.log(e),s.$message({type:"error",message:"请求异常",duration:2e3})})})},getKafkaBootstrapServers:function(){var e=this;e.$http.get(e.api.getKafkaBootstrapServers,{},function(t){0==t.code?(e.bootstrap_servers=t.content,e.$message({type:"success",message:"查询成功",duration:2e3})):e.$message({type:"error",message:t.msg,duration:2e3})},function(t){e.$message({type:"warning",message:"请求异常",duration:1e3})})},routerToView:function(e,t){var s="";s=s+"group_id="+t+"&bootstrap_servers="+e,window.open("#/ConsumerManagerView?"+s,"_self")},routerToEdit:function(e){var t="";t=t+"id="+e,window.open("#/TBlogModuleEdit?"+t,"_self")},routerToAdd:function(){window.open("#/TBlogModuleAdd","_self")},searchEvent:function(){this.getConsumerGroups()}}},o={render:function(){var e=this,t=e.$createElement,s=e._self._c||t;return s("div",{staticClass:"app-container"},[s("div",{staticClass:"mt20"},[s("el-form",{attrs:{inline:!0,size:"mini"}},[s("el-form-item",{attrs:{label:"bootstrap.servers"}},[s("el-select",{attrs:{placeholder:"请输入kafka地址:"},model:{value:e.bootstrap.servers,callback:function(t){e.$set(e.bootstrap,"servers",t)},expression:"bootstrap.servers"}},e._l(e.bootstrap_servers,function(e,t){return s("el-option",{key:e,attrs:{label:t,value:e}})}),1)],1),e._v(" "),s("el-form-item",[s("el-button",{staticClass:"el-button-search",attrs:{type:"primary"},on:{click:function(t){return e.searchEvent()}}},[e._v("查询")])],1)],1)],1),e._v(" "),s("div",{staticClass:"app-list"},[s("div",{staticClass:"app-tab"},[s("table",[e._m(0),e._v(" "),e._m(1),e._v(" "),s("tbody",e._l(e.consumers,function(t,r){return s("tr",[s("td",[e._v(e._s(r+1))]),e._v(" "),s("td",[e._v(e._s(t.groupId))]),e._v(" "),s("td",[e._v(e._s(t.isSimpleConsumerGroup))]),e._v(" "),s("td",[s("span",{on:{click:function(s){return e.routerToView(e.bootstrap.servers,t.groupId)}}},[e._v("查看")]),e._v(" "),s("span",{on:{click:function(s){return e.deleteConsumerGroup(e.bootstrap.servers,t.groupId)}}},[e._v("删除")])])])}),0)]),e._v(" "),s("p",{directives:[{name:"show",rawName:"v-show",value:0==e.total,expression:"total == 0"}],staticClass:"no-data-tip"},[e._v("没有找到相关数据！")]),e._v(" "),s("div",[s("pre",[e._v(e._s(e.jsonData))])])]),e._v(" "),s("div",{directives:[{name:"show",rawName:"v-show",value:e.total>0,expression:"total > 0"}],staticClass:"mt20"},[s("el-pagination",{attrs:{background:"","current-page":e.currentPage,"page-size":e.pageSize,layout:"total,prev, pager, next",total:e.total},on:{"current-change":e.handleCurrentChange,"update:currentPage":function(t){e.currentPage=t},"update:current-page":function(t){e.currentPage=t}}})],1)])])},staticRenderFns:[function(){var e=this.$createElement,t=this._self._c||e;return t("thead",[t("tr",[t("th",[this._v("id")]),this._v(" "),t("th",[this._v("group.id")]),this._v(" "),t("th",[this._v("isSimpleConsumerGroup")]),this._v(" "),t("th",[this._v("操作")])])])},function(){var e=this.$createElement,t=this._self._c||e;return t("tr",[t("th",[this._v("序号")]),this._v(" "),t("th",[this._v("consumer的group.id")]),this._v(" "),t("th",[this._v("是否是简单消费者")]),this._v(" "),t("th",[this._v("操作")])])}]};var n=s("VU/8")(r,o,!1,function(e){s("czpJ")},null,null);t.default=n.exports}});
//# sourceMappingURL=15.7339d6314da8aea723ca.js.map