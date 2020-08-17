include $(TOPDIR)/rules.mk

PKG_NAME:=obuspa
PKG_VERSION:=3.0.0
PKG_RELEASE:=1
PKG_LICENSE:=BSD-3-Clause
PKG_LICENSE_FILES:=LICENSE

PKG_MAINTAINER:=Daniel Danzberger <daniel@dd-wrt.com>

PKG_FIXUP:=autoreconf
CONFIG_SRC_TREE_OVERRIDE:=y

include $(INCLUDE_DIR)/package.mk

define Package/obuspa
  CATEGORY:=Purple
  TITLE:=USP agent
  DEPENDS:=+libopenssl +libcurl +zlib \
		+libmosquitto +libsqlite3 \
		+libubox +libubus +libblobmsg-json +bbfd
endef

define Package/obuspa/description
  Boardband forum USP Agent reference implementation
endef

TARGET_CFLAGS += -D_GNU_SOURCE

ifneq ($(CONFIG_USE_MUSL),)
TARGET_CFLAGS += -DUSE_MUSL
endif

define Package/obuspa/install
	$(INSTALL_DIR) $(1)/usr/sbin $(1)/etc/obuspa
	$(INSTALL_BIN) $(PKG_BUILD_DIR)/obuspa $(1)/usr/sbin/
	$(CP) ./files/* $(1)/
endef

$(eval $(call BuildPackage,obuspa))
